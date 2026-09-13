-- +goose Up
-- Layer 3 of the three-layer quota scheme (ARCHITECTURE.md §2.9,
-- phase-6-multi-tenant-saas.md Task 6): the last-resort backstop, expected
-- to essentially never fire if Layer 1 (API server, quota.go) and Layer 2
-- (scheduler, placement.go's checkQuota) are correct — a trigger on
-- containers/projects insert rejecting anything that would push the owning
-- org's post-insert count past its resource_quotas row, no matter what
-- inserted the row or what session state (if any) it had.
--
-- Both trigger functions are SECURITY DEFINER, deliberately — not for the
-- usual "escape a self-referential RLS policy" reason
-- (0001_organizations_users_memberships.sql's is_org_admin), but because a
-- backstop that depended on the *inserting* session having
-- app.current_org_id/current_user_id correctly set would defend against
-- nothing: the scheduler's own connection (services/scheduler/main.go's
-- admin pool) never sets those at all, and a direct SQL insert bypassing
-- both higher layers — exactly the case this layer exists for — has no
-- reason to either. SECURITY DEFINER makes both functions' own internal
-- reads of resource_quotas/deployments/projects run as the migration's
-- owning role, unconditionally visible, independent of whatever session
-- fired the insert.
-- +goose StatementBegin
CREATE FUNCTION enforce_container_quota() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    v_org_id UUID;
    v_max INT;
    v_count INT;
BEGIN
    SELECT org_id INTO v_org_id FROM deployments WHERE id = NEW.deployment_id;
    -- Both NULL cases below (no such deployment, no such quota row) are
    -- left to whatever else already governs them — deployment_id's own FK
    -- constraint, and every org getting a quota row atomically at signup
    -- (handleSignup) — rather than this trigger inventing a second opinion
    -- about a condition it isn't the layer responsible for.
    IF v_org_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT max_containers INTO v_max FROM resource_quotas WHERE org_id = v_org_id;
    IF v_max IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT count(*) INTO v_count FROM containers c
        JOIN deployments d ON c.deployment_id = d.id
        WHERE d.org_id = v_org_id AND c.status IN ('pending', 'running');

    IF v_count + 1 > v_max THEN
        RAISE EXCEPTION 'quota exceeded: organization % would have % containers, max is %', v_org_id, v_count + 1, v_max
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER containers_quota_check
    BEFORE INSERT ON containers
    FOR EACH ROW EXECUTE FUNCTION enforce_container_quota();

-- +goose StatementBegin
CREATE FUNCTION enforce_project_quota() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    v_max INT;
    v_count INT;
BEGIN
    SELECT max_projects INTO v_max FROM resource_quotas WHERE org_id = NEW.org_id;
    IF v_max IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT count(*) INTO v_count FROM projects WHERE org_id = NEW.org_id;

    IF v_count + 1 > v_max THEN
        RAISE EXCEPTION 'quota exceeded: organization % would have % projects, max is %', NEW.org_id, v_count + 1, v_max
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER projects_quota_check
    BEFORE INSERT ON projects
    FOR EACH ROW EXECUTE FUNCTION enforce_project_quota();

-- +goose Down
DROP TRIGGER projects_quota_check ON projects;
DROP FUNCTION enforce_project_quota();
DROP TRIGGER containers_quota_check ON containers;
DROP FUNCTION enforce_container_quota();
