-- SEG Leads Generation Project: keep-alive function, migration 004 (draft_v1)
-- Called weekly by .github/workflows/supabase-keepalive.yml so the free-plan project
-- never sits idle long enough to pause. Returns counts only, never lead data.

create or replace function public.keepalive()
returns jsonb
language sql stable
set search_path = ''
as $$
  select jsonb_build_object(
    'ok', true,
    'checked_at', now(),
    'leads', (select count(*) from public.leads),
    'runs', (select count(*) from public.token_usage)
  )
$$;

revoke execute on function public.keepalive() from public, anon, authenticated;
grant execute on function public.keepalive() to service_role;

-- Undo:
-- drop function if exists public.keepalive();
