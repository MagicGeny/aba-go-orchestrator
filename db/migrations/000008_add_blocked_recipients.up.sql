CREATE TABLE tenant_blocked_recipients (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    phone_normalized VARCHAR(20) NOT NULL,
    blocked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, phone_normalized)
);

CREATE INDEX idx_tenant_blocked_recipients_tenant_phone ON tenant_blocked_recipients(tenant_id, phone_normalized);

UPDATE outbox_messages o
SET payload = jsonb_set(o.payload, '{tenant_id}', to_jsonb(c.tenant_id::text), true)
FROM campaigns c
WHERE o.event_type = 'message.send'
  AND (o.payload ? 'tenant_id') = false
  AND (o.payload->>'campaign_id') = c.id::text;
