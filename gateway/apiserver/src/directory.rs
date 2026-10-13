//! Complete node views expire independently of the watch transport.
use adx_core::scheduling::{PlacementTarget, SchedulingPolicy};
use adx_protocol::control as pb;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
#[derive(Default)]
pub(crate) struct Directory {
    frame: pb::NodeDirectory,
    valid_until: Option<Instant>,
    next: usize,
}
impl Directory {
    pub fn clear(&mut self) {
        self.frame = pb::NodeDirectory::default();
        self.valid_until = None;
    }
    pub fn update(&mut self, frame: pb::NodeDirectory) -> Result<(), tonic::Status> {
        let mut ids = std::collections::BTreeSet::new();
        if frame.epoch == 0
            || frame.valid_for_millis == 0
            || !valid_host_budgets(&frame)
            || frame.nodes.iter().any(|n| {
                n.node_id.is_empty()
                    || n.address.is_empty()
                    || n.relay_address.is_empty()
                    || n.session_id.is_empty()
                    || !ids.insert(n.node_id.clone())
            })
        {
            self.clear();
            return Err(tonic::Status::data_loss("invalid node directory"));
        }
        self.valid_until =
            Some(Instant::now() + Duration::from_millis(frame.valid_for_millis.min(5000)));
        self.frame = frame;
        Ok(())
    }
    pub fn select(&mut self) -> Option<pb::NodeEndpoint> {
        if self.valid_until.is_none_or(|t| Instant::now() >= t) {
            return None;
        }
        let available = self
            .frame
            .nodes
            .iter()
            .filter(|node| node.accepting_allocations)
            .count();
        if available == 0 {
            return None;
        }
        self.next %= available;
        let node = self
            .frame
            .nodes
            .iter()
            .filter(|node| node.accepting_allocations)
            .nth(self.next)
            .cloned();
        self.next += 1;
        node
    }
    /// Keep hard node placement on its only eligible entry node. The node
    /// still performs authoritative local admission and Coordinator claim;
    /// this only avoids a guaranteed miss and central forwarding hop.
    pub fn select_for(&mut self, spec: &pb::EnvironmentSpec) -> Option<pb::NodeEndpoint> {
        let Some(scheduling) = spec.scheduling.clone() else {
            return self.select();
        };
        let Ok(policy): Result<SchedulingPolicy, _> = scheduling.try_into() else {
            return self.select();
        };
        let has_hard_node = !policy.required_node.is_empty()
            || policy
                .placement_groups
                .iter()
                .any(|group| group.required && group.target == PlacementTarget::Node);
        if !has_hard_node {
            return self.select();
        }
        if self.valid_until.is_none_or(|t| Instant::now() >= t) {
            return None;
        }
        let matches = self
            .frame
            .nodes
            .iter()
            .filter(|node| node.accepting_allocations)
            .filter(|node| {
                let mut labels: BTreeMap<_, _> = node.labels.clone().into_iter().collect();
                labels.insert("NODE_ID".into(), node.node_id.clone());
                policy.matches_node(&labels)
                    && policy
                        .placement_groups
                        .iter()
                        .filter(|group| group.required && group.target == PlacementTarget::Node)
                        .all(|group| {
                            let matched = group
                                .terms
                                .iter()
                                .any(|term| term.selector.matches(&labels));
                            if group.anti {
                                !matched
                            } else {
                                matched
                            }
                        })
            })
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return self.select();
        }
        self.next %= matches.len();
        let node = matches.get(self.next).cloned();
        self.next += 1;
        node
    }
    pub fn snapshot(&self) -> Option<pb::NodeDirectory> {
        if self.valid_until.is_none_or(|t| Instant::now() >= t) {
            return None;
        }
        Some(self.frame.clone())
    }
}
// 完整校验在缓存边界完成；无效消息撤销上一份视图。
fn valid_host_budgets(frame: &pb::NodeDirectory) -> bool {
    use std::collections::BTreeSet;
    if frame.host_budget_configured == frame.host_budgets.is_empty() {
        return false;
    }
    let valid_id = |id: &str| !id.is_empty() && id.trim() == id;
    let mut hosts = BTreeSet::new();
    let mut members = BTreeSet::new();
    let mut totals = [0u64; 3];
    for host in &frame.host_budgets {
        if !valid_id(&host.host_id)
            || !hosts.insert(&host.host_id)
            || host.nodes.is_empty()
            || host
                .nodes
                .iter()
                .any(|id| !valid_id(id) || !members.insert(id))
        {
            return false;
        }
        let (Some(capacity), Some(held), Some(available)) =
            (&host.capacity, &host.held, &host.available)
        else {
            return false;
        };
        if capacity.cpu_millis == 0 || capacity.memory_bytes == 0 {
            return false;
        }
        for (index, (cap, used, free)) in [
            (capacity.cpu_millis, held.cpu_millis, available.cpu_millis),
            (
                capacity.memory_bytes,
                held.memory_bytes,
                available.memory_bytes,
            ),
            (capacity.disk_bytes, held.disk_bytes, available.disk_bytes),
        ]
        .into_iter()
        .enumerate()
        {
            if used.checked_add(free) != Some(cap) {
                return false;
            }
            let Some(total) = totals[index].checked_add(cap) else {
                return false;
            };
            totals[index] = total;
        }
    }
    !frame.host_budget_configured
        || frame.nodes.iter().all(|node| {
            !node.accepting_allocations
                || (members.contains(&node.node_id)
                    && node.capacity.is_some()
                    && node.allocatable.is_some())
        })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotates_only_fresh_full_views_and_replaces_sessions() {
        let mut d = Directory::default();
        assert!(d.select().is_none());
        let node = |id: &str| pb::NodeEndpoint {
            node_id: id.into(),
            address: format!("{id}:9000"),
            relay_address: format!("{id}:9443"),
            session_id: "boot".into(),
            accepting_allocations: true,
            ..Default::default()
        };
        d.update(pb::NodeDirectory {
            epoch: 1,
            nodes: vec![node("a"), node("b")],
            valid_for_millis: 1000,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            d.snapshot()
                .unwrap()
                .nodes
                .into_iter()
                .map(|node| node.node_id)
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(
            (
                d.select().unwrap().node_id,
                d.select().unwrap().node_id,
                d.select().unwrap().node_id
            ),
            ("a".into(), "b".into(), "a".into())
        );
        d.valid_until = Some(Instant::now());
        assert!(d.select().is_none());
        assert!(d.snapshot().is_none());
        d.update(pb::NodeDirectory {
            epoch: 2,
            nodes: vec![pb::NodeEndpoint {
                session_id: "new".into(),
                ..node("a")
            }],
            valid_for_millis: 1000,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(d.select().unwrap().session_id, "new");
        d.update(pb::NodeDirectory {
            epoch: 3,
            nodes: vec![
                pb::NodeEndpoint {
                    accepting_allocations: false,
                    ..node("paused")
                },
                node("ready"),
            ],
            valid_for_millis: 1000,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(d.snapshot().unwrap().nodes.len(), 2);
        assert_eq!(d.select().unwrap().node_id, "ready");
        d.clear();
        assert!(d.select().is_none());
        assert!(d.snapshot().is_none());
        let mut incomplete = node("missing-relay");
        incomplete.relay_address.clear();
        assert_eq!(
            d.update(pb::NodeDirectory {
                epoch: 4,
                nodes: vec![incomplete],
                valid_for_millis: 1000,
                ..Default::default()
            })
            .unwrap_err()
            .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn hard_node_pin_selects_its_entry_node() {
        let mut d = Directory::default();
        let node = |id: &str| pb::NodeEndpoint {
            node_id: id.into(),
            address: format!("{id}:9000"),
            relay_address: format!("{id}:9443"),
            session_id: "boot".into(),
            accepting_allocations: true,
            ..Default::default()
        };
        d.update(pb::NodeDirectory {
            epoch: 1,
            nodes: vec![node("a"), node("b")],
            valid_for_millis: 1000,
            ..Default::default()
        })
        .unwrap();
        let spec = pb::EnvironmentSpec {
            scheduling: Some(pb::SchedulingPolicy {
                placement_groups: vec![pb::PlacementGroup {
                    target: pb::PlacementTarget::Node as i32,
                    terms: vec![pb::WeightedSelector {
                        selector: Some(pb::LabelSelector {
                            expressions: vec![pb::LabelRequirement {
                                key: "NODE_ID".into(),
                                op: pb::SelectorOp::In as i32,
                                values: vec!["b".into()],
                            }],
                            ..Default::default()
                        }),
                        weight: 1,
                    }],
                    required: true,
                    anti: false,
                    ordered: false,
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(d.select_for(&spec).unwrap().node_id, "b");
    }

    #[test]
    fn unconstrained_create_keeps_round_robin_selection() {
        let mut d = Directory::default();
        let node = |id: &str| pb::NodeEndpoint {
            node_id: id.into(),
            address: format!("{id}:9000"),
            relay_address: format!("{id}:9443"),
            session_id: "boot".into(),
            accepting_allocations: true,
            ..Default::default()
        };
        d.update(pb::NodeDirectory {
            epoch: 1,
            nodes: vec![node("a"), node("b")],
            valid_for_millis: 1000,
            ..Default::default()
        })
        .unwrap();
        let spec = pb::EnvironmentSpec::default();

        assert_eq!(d.select_for(&spec).unwrap().node_id, "a");
        assert_eq!(d.select_for(&spec).unwrap().node_id, "b");
    }
}

#[cfg(test)]
#[path = "directory/host_budgets.rs"]
mod host_budget_tests;
