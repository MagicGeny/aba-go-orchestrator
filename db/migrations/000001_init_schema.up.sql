-- Делаем простую обертку, чтобы код таблиц не падал на любой версии Postgres
CREATE OR REPLACE FUNCTION uuidv7() 
RETURNS uuid AS 'SELECT gen_random_uuid();' 
LANGUAGE sql VOLATILE;


-- 1. Типы бизнеса (Тенантов) 
CREATE TYPE tenant_type_enum AS ENUM ('beauty', 'auto_service', 'fitness', 'medical', 'retail', 'generic'); 
CREATE TYPE campaign_status_enum AS ENUM ('draft', 'processing', 'completed', 'paused', 'failed'); 
CREATE TYPE task_status_enum AS ENUM ('pending', 'sent', 'delivered', 'failed', 'replied'); 
 
-- 2. Таблица Компаний/Тенантов (Салоны, Автосервисы и т.д.) 
CREATE TABLE tenants ( 
    id UUID PRIMARY KEY DEFAULT uuidv7(), 
    name VARCHAR(255) NOT NULL, 
    type tenant_type_enum NOT NULL DEFAULT 'generic', 
    admin_phone VARCHAR(20) NOT NULL, -- Телефон для уведомлений 
    admin_messenger VARCHAR(50) NOT NULL DEFAULT 'max', 
    keycloak_group_id VARCHAR(255) UNIQUE, -- Привязка к роли/группе в Keycloak 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP 
); 
 
-- 3. Таблица Маркетинговых Кампаний 
CREATE TABLE campaigns ( 
    id UUID PRIMARY KEY DEFAULT uuidv7(), 
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE, 
    name VARCHAR(255) NOT NULL, 
    message_template TEXT NOT NULL, -- Шаблон с плейсхолдерами, например: "Привет, {name}!" 
    status campaign_status_enum NOT NULL DEFAULT 'draft', 
    original_excel_name VARCHAR(255) NOT NULL, 
    processed_count INT NOT NULL DEFAULT 0, 
    total_count INT NOT NULL DEFAULT 0, 
    error_count INT NOT NULL DEFAULT 0, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP 
); 
 
-- 4. Таблица Контактов Получателей (Абстракция над клиентами) 
CREATE TABLE campaign_targets ( 
    id UUID PRIMARY KEY DEFAULT uuidv7(), 
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE, 
    client_name VARCHAR(255) NOT NULL, 
    phone_normalized VARCHAR(20) NOT NULL, -- Очищенный номер (10 цифр для Max) 
    excel_row_index INT NOT NULL, -- Индекс строки в исходном Excel для обратной сборки 
    status task_status_enum NOT NULL DEFAULT 'pending', 
    last_error TEXT, 
    sent_at TIMESTAMP WITH TIME ZONE, 
    replied_at TIMESTAMP WITH TIME ZONE, 
    last_reply_text TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP 
); 
 
-- 5. Техническая таблица паттерна Transactional Outbox 
CREATE TABLE outbox_messages ( 
    id UUID PRIMARY KEY DEFAULT uuidv7(), 
    event_type VARCHAR(100) NOT NULL, -- например, 'message.send' 
    payload JSONB NOT NULL, 
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, processed, failed 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
    processed_at TIMESTAMP WITH TIME ZONE 
); 
 
-- Индексы для оптимизации 
CREATE INDEX idx_campaign_targets_campaign ON campaign_targets(campaign_id); 
CREATE INDEX idx_campaign_targets_lookup ON campaign_targets(phone_normalized, status); 
CREATE INDEX idx_outbox_pending ON outbox_messages(status) WHERE status = 'pending';

-- Добавляем тестового тенанта для разработки
INSERT INTO tenants (id, name, type, admin_phone, admin_messenger)
VALUES ('00000000-0000-0000-0000-000000000000', 'Test Tenant', 'generic', '79000000000', 'max')
ON CONFLICT (id) DO NOTHING;
