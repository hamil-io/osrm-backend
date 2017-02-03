#ifndef SHARED_MONITOR_HPP
#define SHARED_MONITOR_HPP

#include "storage/shared_datatype.hpp"

#include <boost/format.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

namespace osrm
{
namespace storage
{

namespace
{
namespace bi = boost::interprocess;

template <class Lock> class InvertedLock
{
    Lock &lock;

  public:
    InvertedLock(Lock &lock) : lock(lock) { lock.unlock(); }
    ~InvertedLock() { lock.lock(); }
};
}

// The shared monitor [1] implementation based on the reusable barrier solution [2]
// with two turnstiles based on interprocess semaphores.
// References:
//  1. Silberschatz, Galvin & Gagne. Operating System Concepts. (2004). Section 6.7.3
//  2. Downey. The Little Book of Semaphores. (2008). Section 3.7.5
template <typename Data> struct SharedMonitor
{
    using mutex_type = bi::interprocess_mutex;

    SharedMonitor(const Data &initial_data)
    {
        shmem = bi::shared_memory_object(bi::open_or_create, Data::name, bi::read_write);

        bi::offset_t size = 0;
        if (shmem.get_size(size) && size == 0)
        {
            shmem.truncate(internal_size + sizeof(Data));
            region = bi::mapped_region(shmem, bi::read_write);
            new (&internal()) InternalData;
            new (&data()) Data(initial_data);
        }
        else
        {
            region = bi::mapped_region(shmem, bi::read_write);
        }
    }

    SharedMonitor()
    {
        try
        {
            shmem = bi::shared_memory_object(bi::open_only, Data::name, bi::read_write);

            bi::offset_t size = 0;
            if (!shmem.get_size(size) || size != internal_size + sizeof(Data))
            {
                auto message =
                    boost::format("Wrong shared memory block '%1%' size %2%, expected %3% bytes") %
                    (const char *)Data::name % size % (internal_size + sizeof(Data));
                throw util::exception(message.str() + SOURCE_REF);
            }

            region = bi::mapped_region(shmem, bi::read_write);
        }
        catch (const bi::interprocess_exception &exception)
        {
            auto message = boost::format("No shared memory block '%1%' found, have you forgotten "
                                         "to run osrm-datastore?") %
                           (const char *)Data::name;
            throw util::exception(message.str() + SOURCE_REF);
        }
    }

    Data &data() const
    {
        return *reinterpret_cast<Data *>(reinterpret_cast<char *>(region.get_address()) +
                                         internal_size);
    }

    std::uint32_t get_waiters() const { return internal().waiters; }

    mutex_type &get_mutex() const { return internal().mutex; }

    template <typename Lock> void wait(Lock &lock)
    {
        InvertedLock<Lock> inverted_lock(lock);

        // entrée point
        internal().turnstile_entree.wait();
        internal().turnstile_entree.post();

        ++internal().waiters;

        // rendezvous point
        internal().turnstile_rendezvous.wait();
        internal().turnstile_rendezvous.post();

        // the last waiter closes waiters turnstile and opens entrance one
        if (--internal().waiters == 0)
        {
            internal().turnstile_rendezvous.wait(); // close rendezvous turnstile
            internal().turnstile_entree.post();     // open entree turnstile
        }
    }

    void notify_all()
    {
        if (internal().waiters > 0)
        {
            internal().turnstile_entree.wait();     // close entree turnstile
            internal().turnstile_rendezvous.post(); // open rendezvous turnstile
        }
    }

    static void remove() { bi::shared_memory_object::remove(Data::name); }

  private:
    static constexpr int internal_size = 128;

    struct InternalData
    {
        InternalData() : waiters(0), turnstile_entree(1), turnstile_rendezvous(0) {}

        std::atomic<std::uint32_t> waiters;
        bi::interprocess_semaphore turnstile_entree;
        bi::interprocess_semaphore turnstile_rendezvous;
        mutex_type mutex;
    };
    static_assert(sizeof(InternalData) <= internal_size, "not enough space to place internal data");
    static_assert(alignof(Data) <= internal_size, "incorrect data alignment");

    InternalData &internal() const
    {
        return *reinterpret_cast<InternalData *>(reinterpret_cast<char *>(region.get_address()));
    }

    bi::shared_memory_object shmem;
    bi::mapped_region region;
};
}
}

#endif // SHARED_MONITOR_HPP
