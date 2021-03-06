#include "engine/routing_algorithms/routing_base.hpp"

namespace osrm
{
namespace engine
{
namespace routing_algorithms
{

void BasicRoutingInterface::RoutingStep(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    SearchEngineData::QueryHeap &forward_heap,
    SearchEngineData::QueryHeap &reverse_heap,
    NodeID &middle_node_id,
    EdgeWeight &upper_bound,
    EdgeWeight min_edge_offset,
    const bool forward_direction,
    const bool stalling,
    const bool force_loop_forward,
    const bool force_loop_reverse) const
{
    const NodeID node = forward_heap.DeleteMin();
    const EdgeWeight weight = forward_heap.GetKey(node);

    if (reverse_heap.WasInserted(node))
    {
        const EdgeWeight new_weight = reverse_heap.GetKey(node) + weight;
        if (new_weight < upper_bound)
        {
            // if loops are forced, they are so at the source
            if ((force_loop_forward && forward_heap.GetData(node).parent == node) ||
                (force_loop_reverse && reverse_heap.GetData(node).parent == node) ||
                // in this case we are looking at a bi-directional way where the source
                // and target phantom are on the same edge based node
                new_weight < 0)
            {
                // check whether there is a loop present at the node
                for (const auto edge : facade->GetAdjacentEdgeRange(node))
                {
                    const EdgeData &data = facade->GetEdgeData(edge);
                    bool forward_directionFlag = (forward_direction ? data.forward : data.backward);
                    if (forward_directionFlag)
                    {
                        const NodeID to = facade->GetTarget(edge);
                        if (to == node)
                        {
                            const EdgeWeight edge_weight = data.weight;
                            const EdgeWeight loop_weight = new_weight + edge_weight;
                            if (loop_weight >= 0 && loop_weight < upper_bound)
                            {
                                middle_node_id = node;
                                upper_bound = loop_weight;
                            }
                        }
                    }
                }
            }
            else
            {
                BOOST_ASSERT(new_weight >= 0);

                middle_node_id = node;
                upper_bound = new_weight;
            }
        }
    }

    // make sure we don't terminate too early if we initialize the weight
    // for the nodes in the forward heap with the forward/reverse offset
    BOOST_ASSERT(min_edge_offset <= 0);
    if (weight + min_edge_offset > upper_bound)
    {
        forward_heap.DeleteAll();
        return;
    }

    // Stalling
    if (stalling)
    {
        for (const auto edge : facade->GetAdjacentEdgeRange(node))
        {
            const EdgeData &data = facade->GetEdgeData(edge);
            const bool reverse_flag = ((!forward_direction) ? data.forward : data.backward);
            if (reverse_flag)
            {
                const NodeID to = facade->GetTarget(edge);
                const EdgeWeight edge_weight = data.weight;

                BOOST_ASSERT_MSG(edge_weight > 0, "edge_weight invalid");

                if (forward_heap.WasInserted(to))
                {
                    if (forward_heap.GetKey(to) + edge_weight < weight)
                    {
                        return;
                    }
                }
            }
        }
    }

    for (const auto edge : facade->GetAdjacentEdgeRange(node))
    {
        const EdgeData &data = facade->GetEdgeData(edge);
        bool forward_directionFlag = (forward_direction ? data.forward : data.backward);
        if (forward_directionFlag)
        {
            const NodeID to = facade->GetTarget(edge);
            const EdgeWeight edge_weight = data.weight;

            BOOST_ASSERT_MSG(edge_weight > 0, "edge_weight invalid");
            const EdgeWeight to_weight = weight + edge_weight;

            // New Node discovered -> Add to Heap + Node Info Storage
            if (!forward_heap.WasInserted(to))
            {
                forward_heap.Insert(to, to_weight, node);
            }
            // Found a shorter Path -> Update weight
            else if (to_weight < forward_heap.GetKey(to))
            {
                // new parent
                forward_heap.GetData(to).parent = node;
                forward_heap.DecreaseKey(to, to_weight);
            }
        }
    }
}

/**
 * Unpacks a single edge (NodeID->NodeID) from the CH graph down to it's original non-shortcut
 * route.
 * @param from the node the CH edge starts at
 * @param to the node the CH edge finishes at
 * @param unpacked_path the sequence of original NodeIDs that make up the expanded CH edge
 */
void BasicRoutingInterface::UnpackEdge(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    const NodeID from,
    const NodeID to,
    std::vector<NodeID> &unpacked_path) const
{
    std::array<NodeID, 2> path{{from, to}};
    UnpackCHPath(
        *facade,
        path.begin(),
        path.end(),
        [&unpacked_path](const std::pair<NodeID, NodeID> &edge, const EdgeData & /* data */) {
            unpacked_path.emplace_back(edge.first);
        });
    unpacked_path.emplace_back(to);
}

void BasicRoutingInterface::RetrievePackedPathFromHeap(
    const SearchEngineData::QueryHeap &forward_heap,
    const SearchEngineData::QueryHeap &reverse_heap,
    const NodeID middle_node_id,
    std::vector<NodeID> &packed_path) const
{
    RetrievePackedPathFromSingleHeap(forward_heap, middle_node_id, packed_path);
    std::reverse(packed_path.begin(), packed_path.end());
    packed_path.emplace_back(middle_node_id);
    RetrievePackedPathFromSingleHeap(reverse_heap, middle_node_id, packed_path);
}

void BasicRoutingInterface::RetrievePackedPathFromSingleHeap(
    const SearchEngineData::QueryHeap &search_heap,
    const NodeID middle_node_id,
    std::vector<NodeID> &packed_path) const
{
    NodeID current_node_id = middle_node_id;
    // all initial nodes will have itself as parent, or a node not in the heap
    // in case of a core search heap. We need a distinction between core entry nodes
    // and start nodes since otherwise start node specific code that assumes
    // node == node.parent (e.g. the loop code) might get actived.
    while (current_node_id != search_heap.GetData(current_node_id).parent &&
           search_heap.WasInserted(search_heap.GetData(current_node_id).parent))
    {
        current_node_id = search_heap.GetData(current_node_id).parent;
        packed_path.emplace_back(current_node_id);
    }
}

// assumes that heaps are already setup correctly.
// ATTENTION: This only works if no additional offset is supplied next to the Phantom Node
// Offsets.
// In case additional offsets are supplied, you might have to force a loop first.
// A forced loop might be necessary, if source and target are on the same segment.
// If this is the case and the offsets of the respective direction are larger for the source
// than the target
// then a force loop is required (e.g. source_phantom.forward_segment_id ==
// target_phantom.forward_segment_id
// && source_phantom.GetForwardWeightPlusOffset() > target_phantom.GetForwardWeightPlusOffset())
// requires
// a force loop, if the heaps have been initialized with positive offsets.
void BasicRoutingInterface::Search(const std::shared_ptr<const datafacade::BaseDataFacade> facade,
                                   SearchEngineData::QueryHeap &forward_heap,
                                   SearchEngineData::QueryHeap &reverse_heap,
                                   EdgeWeight &weight,
                                   std::vector<NodeID> &packed_leg,
                                   const bool force_loop_forward,
                                   const bool force_loop_reverse,
                                   const EdgeWeight weight_upper_bound) const
{
    NodeID middle = SPECIAL_NODEID;
    weight = weight_upper_bound;

    // get offset to account for offsets on phantom nodes on compressed edges
    const auto min_edge_offset = std::min(0, forward_heap.MinKey());
    BOOST_ASSERT(min_edge_offset <= 0);
    // we only every insert negative offsets for nodes in the forward heap
    BOOST_ASSERT(reverse_heap.MinKey() >= 0);

    // run two-Target Dijkstra routing step.
    const constexpr bool STALLING_ENABLED = true;
    while (0 < (forward_heap.Size() + reverse_heap.Size()))
    {
        if (!forward_heap.Empty())
        {
            RoutingStep(facade,
                        forward_heap,
                        reverse_heap,
                        middle,
                        weight,
                        min_edge_offset,
                        true,
                        STALLING_ENABLED,
                        force_loop_forward,
                        force_loop_reverse);
        }
        if (!reverse_heap.Empty())
        {
            RoutingStep(facade,
                        reverse_heap,
                        forward_heap,
                        middle,
                        weight,
                        min_edge_offset,
                        false,
                        STALLING_ENABLED,
                        force_loop_reverse,
                        force_loop_forward);
        }
    }

    // No path found for both target nodes?
    if (weight_upper_bound <= weight || SPECIAL_NODEID == middle)
    {
        weight = INVALID_EDGE_WEIGHT;
        return;
    }

    // Was a paths over one of the forward/reverse nodes not found?
    BOOST_ASSERT_MSG((SPECIAL_NODEID != middle && INVALID_EDGE_WEIGHT != weight), "no path found");

    // make sure to correctly unpack loops
    if (weight != forward_heap.GetKey(middle) + reverse_heap.GetKey(middle))
    {
        // self loop makes up the full path
        packed_leg.push_back(middle);
        packed_leg.push_back(middle);
    }
    else
    {
        RetrievePackedPathFromHeap(forward_heap, reverse_heap, middle, packed_leg);
    }
}

// assumes that heaps are already setup correctly.
// A forced loop might be necessary, if source and target are on the same segment.
// If this is the case and the offsets of the respective direction are larger for the source
// than the target
// then a force loop is required (e.g. source_phantom.forward_segment_id ==
// target_phantom.forward_segment_id
// && source_phantom.GetForwardWeightPlusOffset() > target_phantom.GetForwardWeightPlusOffset())
// requires
// a force loop, if the heaps have been initialized with positive offsets.
void BasicRoutingInterface::SearchWithCore(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    SearchEngineData::QueryHeap &forward_heap,
    SearchEngineData::QueryHeap &reverse_heap,
    SearchEngineData::QueryHeap &forward_core_heap,
    SearchEngineData::QueryHeap &reverse_core_heap,
    EdgeWeight &weight,
    std::vector<NodeID> &packed_leg,
    const bool force_loop_forward,
    const bool force_loop_reverse,
    EdgeWeight weight_upper_bound) const
{
    NodeID middle = SPECIAL_NODEID;
    weight = weight_upper_bound;

    using CoreEntryPoint = std::tuple<NodeID, EdgeWeight, NodeID>;
    std::vector<CoreEntryPoint> forward_entry_points;
    std::vector<CoreEntryPoint> reverse_entry_points;

    // get offset to account for offsets on phantom nodes on compressed edges
    const auto min_edge_offset = std::min(0, forward_heap.MinKey());
    // we only every insert negative offsets for nodes in the forward heap
    BOOST_ASSERT(reverse_heap.MinKey() >= 0);

    const constexpr bool STALLING_ENABLED = true;
    // run two-Target Dijkstra routing step.
    while (0 < (forward_heap.Size() + reverse_heap.Size()))
    {
        if (!forward_heap.Empty())
        {
            if (facade->IsCoreNode(forward_heap.Min()))
            {
                const NodeID node = forward_heap.DeleteMin();
                const EdgeWeight key = forward_heap.GetKey(node);
                forward_entry_points.emplace_back(node, key, forward_heap.GetData(node).parent);
            }
            else
            {
                RoutingStep(facade,
                            forward_heap,
                            reverse_heap,
                            middle,
                            weight,
                            min_edge_offset,
                            true,
                            STALLING_ENABLED,
                            force_loop_forward,
                            force_loop_reverse);
            }
        }
        if (!reverse_heap.Empty())
        {
            if (facade->IsCoreNode(reverse_heap.Min()))
            {
                const NodeID node = reverse_heap.DeleteMin();
                const EdgeWeight key = reverse_heap.GetKey(node);
                reverse_entry_points.emplace_back(node, key, reverse_heap.GetData(node).parent);
            }
            else
            {
                RoutingStep(facade,
                            reverse_heap,
                            forward_heap,
                            middle,
                            weight,
                            min_edge_offset,
                            false,
                            STALLING_ENABLED,
                            force_loop_reverse,
                            force_loop_forward);
            }
        }
    }

    const auto insertInCoreHeap = [](const CoreEntryPoint &p,
                                     SearchEngineData::QueryHeap &core_heap) {
        NodeID id;
        EdgeWeight weight;
        NodeID parent;
        // TODO this should use std::apply when we get c++17 support
        std::tie(id, weight, parent) = p;
        core_heap.Insert(id, weight, parent);
    };

    forward_core_heap.Clear();
    for (const auto &p : forward_entry_points)
    {
        insertInCoreHeap(p, forward_core_heap);
    }

    reverse_core_heap.Clear();
    for (const auto &p : reverse_entry_points)
    {
        insertInCoreHeap(p, reverse_core_heap);
    }

    // get offset to account for offsets on phantom nodes on compressed edges
    EdgeWeight min_core_edge_offset = 0;
    if (forward_core_heap.Size() > 0)
    {
        min_core_edge_offset = std::min(min_core_edge_offset, forward_core_heap.MinKey());
    }
    if (reverse_core_heap.Size() > 0 && reverse_core_heap.MinKey() < 0)
    {
        min_core_edge_offset = std::min(min_core_edge_offset, reverse_core_heap.MinKey());
    }
    BOOST_ASSERT(min_core_edge_offset <= 0);

    // run two-target Dijkstra routing step on core with termination criterion
    const constexpr bool STALLING_DISABLED = false;
    while (0 < forward_core_heap.Size() && 0 < reverse_core_heap.Size() &&
           weight > (forward_core_heap.MinKey() + reverse_core_heap.MinKey()))
    {
        RoutingStep(facade,
                    forward_core_heap,
                    reverse_core_heap,
                    middle,
                    weight,
                    min_core_edge_offset,
                    true,
                    STALLING_DISABLED,
                    force_loop_forward,
                    force_loop_reverse);

        RoutingStep(facade,
                    reverse_core_heap,
                    forward_core_heap,
                    middle,
                    weight,
                    min_core_edge_offset,
                    false,
                    STALLING_DISABLED,
                    force_loop_reverse,
                    force_loop_forward);
    }

    // No path found for both target nodes?
    if (weight_upper_bound <= weight || SPECIAL_NODEID == middle)
    {
        weight = INVALID_EDGE_WEIGHT;
        return;
    }

    // Was a paths over one of the forward/reverse nodes not found?
    BOOST_ASSERT_MSG((SPECIAL_NODEID != middle && INVALID_EDGE_WEIGHT != weight), "no path found");

    // we need to unpack sub path from core heaps
    if (facade->IsCoreNode(middle))
    {
        if (weight != forward_core_heap.GetKey(middle) + reverse_core_heap.GetKey(middle))
        {
            // self loop
            BOOST_ASSERT(forward_core_heap.GetData(middle).parent == middle &&
                         reverse_core_heap.GetData(middle).parent == middle);
            packed_leg.push_back(middle);
            packed_leg.push_back(middle);
        }
        else
        {
            std::vector<NodeID> packed_core_leg;
            RetrievePackedPathFromHeap(
                forward_core_heap, reverse_core_heap, middle, packed_core_leg);
            BOOST_ASSERT(packed_core_leg.size() > 0);
            RetrievePackedPathFromSingleHeap(forward_heap, packed_core_leg.front(), packed_leg);
            std::reverse(packed_leg.begin(), packed_leg.end());
            packed_leg.insert(packed_leg.end(), packed_core_leg.begin(), packed_core_leg.end());
            RetrievePackedPathFromSingleHeap(reverse_heap, packed_core_leg.back(), packed_leg);
        }
    }
    else
    {
        if (weight != forward_heap.GetKey(middle) + reverse_heap.GetKey(middle))
        {
            // self loop
            BOOST_ASSERT(forward_heap.GetData(middle).parent == middle &&
                         reverse_heap.GetData(middle).parent == middle);
            packed_leg.push_back(middle);
            packed_leg.push_back(middle);
        }
        else
        {
            RetrievePackedPathFromHeap(forward_heap, reverse_heap, middle, packed_leg);
        }
    }
}

bool BasicRoutingInterface::NeedsLoopForward(const PhantomNode &source_phantom,
                                             const PhantomNode &target_phantom) const
{
    return source_phantom.forward_segment_id.enabled && target_phantom.forward_segment_id.enabled &&
           source_phantom.forward_segment_id.id == target_phantom.forward_segment_id.id &&
           source_phantom.GetForwardWeightPlusOffset() >
               target_phantom.GetForwardWeightPlusOffset();
}

bool BasicRoutingInterface::NeedsLoopBackwards(const PhantomNode &source_phantom,
                                               const PhantomNode &target_phantom) const
{
    return source_phantom.reverse_segment_id.enabled && target_phantom.reverse_segment_id.enabled &&
           source_phantom.reverse_segment_id.id == target_phantom.reverse_segment_id.id &&
           source_phantom.GetReverseWeightPlusOffset() >
               target_phantom.GetReverseWeightPlusOffset();
}

double BasicRoutingInterface::GetPathDistance(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    const std::vector<NodeID> &packed_path,
    const PhantomNode &source_phantom,
    const PhantomNode &target_phantom) const
{
    std::vector<PathData> unpacked_path;
    PhantomNodes nodes;
    nodes.source_phantom = source_phantom;
    nodes.target_phantom = target_phantom;
    UnpackPath(facade, packed_path.begin(), packed_path.end(), nodes, unpacked_path);

    using util::coordinate_calculation::detail::DEGREE_TO_RAD;
    using util::coordinate_calculation::detail::EARTH_RADIUS;

    double distance = 0;
    double prev_lat = static_cast<double>(toFloating(source_phantom.location.lat)) * DEGREE_TO_RAD;
    double prev_lon = static_cast<double>(toFloating(source_phantom.location.lon)) * DEGREE_TO_RAD;
    double prev_cos = std::cos(prev_lat);
    for (const auto &p : unpacked_path)
    {
        const auto current_coordinate = facade->GetCoordinateOfNode(p.turn_via_node);

        const double current_lat =
            static_cast<double>(toFloating(current_coordinate.lat)) * DEGREE_TO_RAD;
        const double current_lon =
            static_cast<double>(toFloating(current_coordinate.lon)) * DEGREE_TO_RAD;
        const double current_cos = std::cos(current_lat);

        const double sin_dlon = std::sin((prev_lon - current_lon) / 2.0);
        const double sin_dlat = std::sin((prev_lat - current_lat) / 2.0);

        const double aharv = sin_dlat * sin_dlat + prev_cos * current_cos * sin_dlon * sin_dlon;
        const double charv = 2. * std::atan2(std::sqrt(aharv), std::sqrt(1.0 - aharv));
        distance += EARTH_RADIUS * charv;

        prev_lat = current_lat;
        prev_lon = current_lon;
        prev_cos = current_cos;
    }

    const double current_lat =
        static_cast<double>(toFloating(target_phantom.location.lat)) * DEGREE_TO_RAD;
    const double current_lon =
        static_cast<double>(toFloating(target_phantom.location.lon)) * DEGREE_TO_RAD;
    const double current_cos = std::cos(current_lat);

    const double sin_dlon = std::sin((prev_lon - current_lon) / 2.0);
    const double sin_dlat = std::sin((prev_lat - current_lat) / 2.0);

    const double aharv = sin_dlat * sin_dlat + prev_cos * current_cos * sin_dlon * sin_dlon;
    const double charv = 2. * std::atan2(std::sqrt(aharv), std::sqrt(1.0 - aharv));
    distance += EARTH_RADIUS * charv;

    return distance;
}

// Requires the heaps for be empty
// If heaps should be adjusted to be initialized outside of this function,
// the addition of force_loop parameters might be required
double BasicRoutingInterface::GetNetworkDistanceWithCore(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    SearchEngineData::QueryHeap &forward_heap,
    SearchEngineData::QueryHeap &reverse_heap,
    SearchEngineData::QueryHeap &forward_core_heap,
    SearchEngineData::QueryHeap &reverse_core_heap,
    const PhantomNode &source_phantom,
    const PhantomNode &target_phantom,
    EdgeWeight weight_upper_bound) const
{
    BOOST_ASSERT(forward_heap.Empty());
    BOOST_ASSERT(reverse_heap.Empty());

    if (source_phantom.forward_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.forward_segment_id.id,
                            -source_phantom.GetForwardWeightPlusOffset(),
                            source_phantom.forward_segment_id.id);
    }
    if (source_phantom.reverse_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.reverse_segment_id.id,
                            -source_phantom.GetReverseWeightPlusOffset(),
                            source_phantom.reverse_segment_id.id);
    }

    if (target_phantom.forward_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.forward_segment_id.id,
                            target_phantom.GetForwardWeightPlusOffset(),
                            target_phantom.forward_segment_id.id);
    }
    if (target_phantom.reverse_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.reverse_segment_id.id,
                            target_phantom.GetReverseWeightPlusOffset(),
                            target_phantom.reverse_segment_id.id);
    }

    const bool constexpr DO_NOT_FORCE_LOOPS =
        false; // prevents forcing of loops, since offsets are set correctly

    EdgeWeight weight = INVALID_EDGE_WEIGHT;
    std::vector<NodeID> packed_path;
    SearchWithCore(facade,
                   forward_heap,
                   reverse_heap,
                   forward_core_heap,
                   reverse_core_heap,
                   weight,
                   packed_path,
                   DO_NOT_FORCE_LOOPS,
                   DO_NOT_FORCE_LOOPS,
                   weight_upper_bound);

    double distance = std::numeric_limits<double>::max();
    if (weight != INVALID_EDGE_WEIGHT)
    {
        return GetPathDistance(facade, packed_path, source_phantom, target_phantom);
    }
    return distance;
}

// Requires the heaps for be empty
// If heaps should be adjusted to be initialized outside of this function,
// the addition of force_loop parameters might be required
double BasicRoutingInterface::GetNetworkDistance(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    SearchEngineData::QueryHeap &forward_heap,
    SearchEngineData::QueryHeap &reverse_heap,
    const PhantomNode &source_phantom,
    const PhantomNode &target_phantom,
    EdgeWeight weight_upper_bound) const
{
    BOOST_ASSERT(forward_heap.Empty());
    BOOST_ASSERT(reverse_heap.Empty());

    if (source_phantom.forward_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.forward_segment_id.id,
                            -source_phantom.GetForwardWeightPlusOffset(),
                            source_phantom.forward_segment_id.id);
    }
    if (source_phantom.reverse_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.reverse_segment_id.id,
                            -source_phantom.GetReverseWeightPlusOffset(),
                            source_phantom.reverse_segment_id.id);
    }

    if (target_phantom.forward_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.forward_segment_id.id,
                            target_phantom.GetForwardWeightPlusOffset(),
                            target_phantom.forward_segment_id.id);
    }
    if (target_phantom.reverse_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.reverse_segment_id.id,
                            target_phantom.GetReverseWeightPlusOffset(),
                            target_phantom.reverse_segment_id.id);
    }

    const bool constexpr DO_NOT_FORCE_LOOPS =
        false; // prevents forcing of loops, since offsets are set correctly

    EdgeWeight weight = INVALID_EDGE_WEIGHT;
    std::vector<NodeID> packed_path;
    Search(facade,
           forward_heap,
           reverse_heap,
           weight,
           packed_path,
           DO_NOT_FORCE_LOOPS,
           DO_NOT_FORCE_LOOPS,
           weight_upper_bound);

    if (weight == INVALID_EDGE_WEIGHT)
    {
        return std::numeric_limits<double>::max();
    }

    return GetPathDistance(facade, packed_path, source_phantom, target_phantom);
}

} // namespace routing_algorithms
} // namespace engine
} // namespace osrm
