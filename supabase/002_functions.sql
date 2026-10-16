-- SEG Leads Generation Project: Supabase functions, migration 002 (draft_v1)
-- Everything here can be undone: see the "Undo" block at the end.

-- 1. Rename to the agreed column name
alter table public.token_usage rename column date_ran to date_run;

-- 2. Company-name normalizer: "The Acme Plastics Co., L.L.C." -> "acme plastics"
--    Lowercases, turns & into "and", strips punctuation and legal suffixes.
create or replace function public.normalize_company_name(p_name text)
returns text
language sql immutable parallel safe
set search_path = ''
as $$
  select nullif(btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
           lower(coalesce(p_name, '')),
           '&', ' and ', 'g'),
           '[^a-z0-9]+', ' ', 'g'),
           '\m(the|inc|incorporated|llc|l l c|ltd|limited|corp|corporation|co|company|lp|llp|pc|pllc|plc)\M', ' ', 'g'),
           '\s+', ' ', 'g')), '')
$$;

-- 3. Leads table: duplicates are judged on the normalized name, not the raw text
alter table public.leads drop constraint if exists leads_company_name_key;
alter table public.leads
  add column if not exists company_key text
    generated always as (public.normalize_company_name(company_name::text)) stored;
alter table public.leads add column if not exists first_seen timestamptz not null default now();
alter table public.leads add constraint leads_company_key_key unique (company_key);
alter table public.leads add constraint leads_company_key_present check (company_key is not null);

-- 4. Read: has this company been seen before?
create or replace function public.is_company_seen(p_company_name text)
returns boolean
language sql stable
set search_path = ''
as $$
  select exists (
    select 1 from public.leads
    where company_key = public.normalize_company_name(p_company_name)
  )
$$;

-- 5. Read, batch: which of these candidate companies are new? (one call per batch)
--    Also removes duplicates inside the batch itself.
create or replace function public.filter_new_companies(p_company_names text[])
returns table (company_name text)
language sql stable
set search_path = ''
as $$
  select distinct on (s.k) s.n
  from (
    select btrim(u.n) as n, public.normalize_company_name(u.n) as k
    from unnest(p_company_names) as u(n)
  ) s
  where s.k is not null
    and not exists (select 1 from public.leads l where l.company_key = s.k)
  order by s.k, s.n
$$;

-- 6. Write: claim a company. True = new and now recorded; false = already seen.
--    Safe if two workers try the same company at the same moment.
create or replace function public.claim_company(p_company_name text)
returns boolean
language plpgsql volatile
set search_path = ''
as $$
declare
  v_id bigint;
begin
  if public.normalize_company_name(p_company_name) is null then
    raise exception 'company name is empty';
  end if;
  insert into public.leads (company_name)
  values (btrim(p_company_name))
  on conflict (company_key) do nothing
  returning id into v_id;
  return v_id is not null;
end
$$;

-- 7. Token tracking: open a run row at the start, add to it after every model call.
--    A run that crashes halfway still keeps the tokens it used.
create or replace function public.start_run(p_date date default current_date)
returns bigint
language sql volatile
set search_path = ''
as $$
  insert into public.token_usage (date_run, tokens_used)
  values (coalesce(p_date, current_date), 0)
  returning id
$$;

create or replace function public.add_tokens(p_run_id bigint, p_tokens int)
returns int
language plpgsql volatile
set search_path = ''
as $$
declare
  v_total int;
begin
  if p_tokens is null or p_tokens < 0 then
    raise exception 'tokens must be zero or more';
  end if;
  update public.token_usage
     set tokens_used = tokens_used + p_tokens
   where id = p_run_id
  returning tokens_used into v_total;
  if v_total is null then
    raise exception 'run % not found', p_run_id;
  end if;
  return v_total;
end
$$;

-- 8. Monthly totals for the cost case study
create or replace view public.token_usage_monthly
with (security_invoker = true) as
select date_trunc('month', date_run)::date as month,
       count(*)                            as runs,
       sum(tokens_used)                    as tokens_used
from public.token_usage
group by 1;

-- 9. Only the agent's secret key may call these
revoke execute on function
  public.normalize_company_name(text), public.is_company_seen(text),
  public.filter_new_companies(text[]), public.claim_company(text),
  public.start_run(date), public.add_tokens(bigint, int)
from public, anon, authenticated;
grant execute on function
  public.normalize_company_name(text), public.is_company_seen(text),
  public.filter_new_companies(text[]), public.claim_company(text),
  public.start_run(date), public.add_tokens(bigint, int)
to service_role;
revoke all on public.token_usage_monthly from anon, authenticated;

-- Undo (run only to reverse this migration):
-- drop view if exists public.token_usage_monthly;
-- drop function if exists public.add_tokens(bigint, int), public.start_run(date),
--   public.claim_company(text), public.filter_new_companies(text[]), public.is_company_seen(text);
-- alter table public.leads drop constraint if exists leads_company_key_present;
-- alter table public.leads drop constraint if exists leads_company_key_key;
-- alter table public.leads drop column if exists first_seen;
-- alter table public.leads drop column if exists company_key;
-- drop function if exists public.normalize_company_name(text);
-- alter table public.leads add constraint leads_company_name_key unique (company_name);
-- alter table public.token_usage rename column date_run to date_ran;
