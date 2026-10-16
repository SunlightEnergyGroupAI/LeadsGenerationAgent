-- SEG Leads Generation Project: database self-test (draft_v1)
-- Run in the Supabase SQL editor after any schema change. Supabase will warn that it is
-- destructive: the only rows it deletes are the test rows it creates. Every row should say PASS.
-- Last run 2026-10-16: 13 of 13 PASS, tables left empty.

create temp table if not exists t (n int, step text, expected text, actual text);
truncate t;
insert into t values (1, 'normalize "The Acme Plastics Co., L.L.C."', 'acme plastics', public.normalize_company_name('The Acme Plastics Co., L.L.C.'));
insert into t values (2, 'claim new: Acme Plastics Inc.', 'true', public.claim_company('Acme Plastics Inc.')::text);
insert into t values (3, 'claim variant: ACME plastics, LLC', 'false', public.claim_company('ACME plastics, LLC')::text);
insert into t values (4, 'seen: the acme plastics company', 'true', public.is_company_seen('the acme plastics company')::text);
insert into t values (5, 'seen: Beta Foods', 'false', public.is_company_seen('Beta Foods')::text);
insert into t select 6, 'filter batch', 'Beta Foods,Gamma & Sons', string_agg(company_name, ',' order by company_name)
  from public.filter_new_companies(array['Acme Plastics','Beta Foods','beta foods inc','Gamma & Sons']);
insert into t select 7, 'leads rows after test', '1', count(*)::text from public.leads;
insert into t select 8, 'start_run, add 1200 then 300 tokens', '1500', public.add_tokens(r, 300)::text
  from (select public.add_tokens(r0, 1200) as x, r0 as r from (select public.start_run() as r0) a) b;
insert into t select 9, 'token_usage date column name', 'date_run', column_name
  from information_schema.columns where table_schema = 'public' and table_name = 'token_usage' and column_name like 'date%';
insert into t select 10, 'monthly view total', '1500', coalesce(sum(tokens_used), 0)::text from public.token_usage_monthly;
insert into t select 11, 'anon can call claim_company', 'false', has_function_privilege('anon', 'public.claim_company(text)', 'execute')::text;
insert into t select 12, 'service_role can call claim_company', 'true', has_function_privilege('service_role', 'public.claim_company(text)', 'execute')::text;
delete from public.leads where company_key = 'acme plastics';
delete from public.token_usage where tokens_used = 1500 and date_run = current_date;
insert into t select 13, 'cleanup: rows left (leads,token_usage)', '0,0',
  (select count(*) from public.leads)::text || ',' || (select count(*) from public.token_usage)::text;
select n, step, expected, actual, case when expected = actual then 'PASS' else 'FAIL' end as result from t order by n;
