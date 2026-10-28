//! Environment template HTTP transport. Lifecycle and ownership stay in Coordinator.
use crate::http::Api;
use adx_observability::trace;
use adx_protocol::control as pb;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use tonic::Status;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Import {
    template_id: String,
    snapshot_id: String,
    name: String,
}

fn value(item: pb::EnvironmentTemplate, caller: &pb::CallerContext) -> Result<Value, Status> {
    if item.tenant_id != caller.tenant_id || item.id.is_empty() {
        return Err(Status::data_loss("template identity mismatch"));
    }
    Ok(
        json!({"templateId":item.id, "snapshotId":item.snapshot_id, "name":item.name,
        "status":if item.deleted {"deleted"} else {"ready"}}),
    )
}

impl Api {
    pub(crate) async fn templates(
        &self,
        method: &str,
        id: Option<&str>,
        query: &HashMap<String, String>,
        input: &Value,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let mut client = pb::snapshot_service_client::SnapshotServiceClient::new(
            self.clients.coordinator().await?,
        );
        match (method, id) {
            ("POST", None) => {
                let r: Import = serde_json::from_value(input.clone()).map_err(|_| {
                    Status::invalid_argument("templateId, snapshotId and name required")
                })?;
                let expected_id = r.template_id.clone();
                let item = self
                    .clients
                    .rpc(
                        "apiserver.import_template",
                        client.import_template(trace::inject(pb::ImportTemplateRequest {
                            template: Some(pb::EnvironmentTemplate {
                                id: r.template_id,
                                snapshot_id: r.snapshot_id,
                                tenant_id: caller.tenant_id.clone(),
                                name: r.name,
                                deleted: false,
                            }),
                            caller: Some(caller.clone()),
                        })),
                    )
                    .await?;
                if item.id != expected_id || item.deleted {
                    return Err(Status::data_loss("template publication not confirmed"));
                }
                value(item, caller)
            }
            ("GET", Some(id)) => {
                let item = self
                    .clients
                    .rpc(
                        "apiserver.get_template",
                        client.get_template(trace::inject(pb::GetTemplateRequest {
                            id: id.into(),
                            caller: Some(caller.clone()),
                        })),
                    )
                    .await?;
                if item.id != id || item.deleted {
                    return Err(Status::data_loss("template lookup identity mismatch"));
                }
                value(item, caller)
            }
            ("DELETE", Some(id)) => {
                let item = self
                    .clients
                    .rpc(
                        "apiserver.delete_template",
                        client.delete_template(trace::inject(pb::GetTemplateRequest {
                            id: id.into(),
                            caller: Some(caller.clone()),
                        })),
                    )
                    .await?;
                if item.id != id || !item.deleted {
                    return Err(Status::data_loss("template deletion not confirmed"));
                }
                value(item, caller)?;
                Ok(json!({}))
            }
            ("GET", None) => {
                let size = query
                    .get("pageSize")
                    .map(|s| s.parse::<u32>())
                    .transpose()
                    .map_err(|_| Status::invalid_argument("invalid page size"))?
                    .unwrap_or(100);
                if size == 0 || size > 1000 {
                    return Err(Status::invalid_argument("invalid page size"));
                }
                let page = self
                    .clients
                    .rpc(
                        "apiserver.list_templates",
                        client.list_templates(trace::inject(pb::ListTemplatesRequest {
                            caller: Some(caller.clone()),
                            name: query.get("name").cloned().unwrap_or_default(),
                            page_token: query.get("pageToken").cloned().unwrap_or_default(),
                            page_size: size,
                        })),
                    )
                    .await?;
                let items = page
                    .templates
                    .into_iter()
                    .map(|t| value(t, caller))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(json!({"items":items,"nextPageToken":page.next_page_token}))
            }
            _ => Err(Status::unimplemented("unsupported template method")),
        }
    }
}
