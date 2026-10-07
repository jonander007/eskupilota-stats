-- ════════════════════════════════════════════════════════════════════
-- PORRA DE ESKUPILOTASTATS
-- Pegar entero en Supabase → SQL Editor → New query → Run.
-- Se puede volver a ejecutar sin perder datos (crea solo lo que falta y
-- rehace reglas, vistas y funciones).
-- ════════════════════════════════════════════════════════════════════

-- ── Perfiles: el alias público de cada usuario ──────────────────────
create table if not exists public.perfiles (
  id      uuid primary key references auth.users on delete cascade,
  alias   text not null check (char_length(alias) between 3 and 20
                               and alias ~ '^[A-Za-z0-9ÁÉÍÓÚÜÑáéíóúüñ _.-]+$'),
  oculto  boolean not null default false,          -- moderación: true lo saca de las clasificaciones
  creado  timestamptz not null default now()
);
create unique index if not exists perfiles_alias_unico on public.perfiles (lower(alias));

-- ── Partidos de la porra (los sube el workflow desde la cartelera) ──
create table if not exists public.porra_partidos (
  id          text primary key,                    -- 2026-10-09_agirre_vs_zubizarreta-iii
  inicio      timestamptz not null,                -- hora de la velada: cierre de pronósticos
  competicion text,
  fase        text,
  fronton     text,
  modalidad   text,
  eq1         text[] not null,
  eq2         text[] not null,
  estado      text not null default 'abierto' check (estado in ('abierto', 'jugado', 'anulado')),
  puntos1     smallint,
  puntos2     smallint,
  actualizado timestamptz not null default now()
);
create index if not exists porra_partidos_inicio on public.porra_partidos (inicio);

-- ── Pronósticos ─────────────────────────────────────────────────────
create table if not exists public.porra_pronosticos (
  usuario         uuid not null default auth.uid() references public.perfiles (id) on delete cascade,
  partido         text not null references public.porra_partidos (id) on delete cascade,
  ganador         smallint not null check (ganador in (1, 2)),
  tantos_perdedor smallint check (tantos_perdedor between 0 and 21),
  actualizado     timestamptz not null default now(),
  primary key (usuario, partido)
);
create index if not exists porra_pronosticos_partido on public.porra_pronosticos (partido);

-- ── Seguridad (Row Level Security) ──────────────────────────────────
alter table public.perfiles          enable row level security;
alter table public.porra_partidos    enable row level security;
alter table public.porra_pronosticos enable row level security;

drop policy if exists "perfiles: ver"      on public.perfiles;
drop policy if exists "perfiles: crear"    on public.perfiles;
drop policy if exists "perfiles: cambiar"  on public.perfiles;
create policy "perfiles: ver"     on public.perfiles for select using (not oculto or id = auth.uid());
create policy "perfiles: crear"   on public.perfiles for insert to authenticated with check (id = auth.uid() and not oculto);
create policy "perfiles: cambiar" on public.perfiles for update to authenticated using (id = auth.uid()) with check (id = auth.uid());

drop policy if exists "partidos: ver" on public.porra_partidos;
create policy "partidos: ver" on public.porra_partidos for select using (true);

-- Los pronósticos ajenos solo se ven cuando el partido ya ha empezado (nadie copia)
-- y solo se puede pronosticar, cambiar o borrar antes de la hora de inicio.
drop policy if exists "pronosticos: ver"     on public.porra_pronosticos;
drop policy if exists "pronosticos: crear"   on public.porra_pronosticos;
drop policy if exists "pronosticos: cambiar" on public.porra_pronosticos;
drop policy if exists "pronosticos: borrar"  on public.porra_pronosticos;
create policy "pronosticos: ver" on public.porra_pronosticos for select using (
  usuario = auth.uid()
  or exists (select 1 from public.porra_partidos p where p.id = partido and p.inicio <= now()));
create policy "pronosticos: crear" on public.porra_pronosticos for insert to authenticated with check (
  usuario = auth.uid()
  and exists (select 1 from public.porra_partidos p where p.id = partido and p.estado = 'abierto' and p.inicio > now()));
create policy "pronosticos: cambiar" on public.porra_pronosticos for update to authenticated
  using (usuario = auth.uid())
  with check (usuario = auth.uid()
  and exists (select 1 from public.porra_partidos p where p.id = partido and p.estado = 'abierto' and p.inicio > now()));
create policy "pronosticos: borrar" on public.porra_pronosticos for delete to authenticated using (
  usuario = auth.uid()
  and exists (select 1 from public.porra_partidos p where p.id = partido and p.estado = 'abierto' and p.inicio > now()));

-- Permisos: el usuario solo puede tocar su alias (no "oculto" ni "creado")
revoke all on public.perfiles, public.porra_partidos, public.porra_pronosticos from anon, authenticated;
grant select on public.perfiles, public.porra_partidos, public.porra_pronosticos to anon, authenticated;
grant insert (id, alias), update (alias) on public.perfiles to authenticated;
grant insert (partido, ganador, tantos_perdedor), update (partido, ganador, tantos_perdedor), delete
  on public.porra_pronosticos to authenticated;
grant all on public.perfiles, public.porra_partidos, public.porra_pronosticos to service_role;

-- ── Puntuación ──────────────────────────────────────────────────────
-- 3 puntos por acertar el ganador; si además se acierta el tanteo del
-- perdedor, +3 (exacto) o +1 (a 2 tantos o menos). Máximo 6.
create or replace view public.porra_puntuados with (security_invoker = true) as
select pr.usuario, pr.partido, pr.ganador, pr.tantos_perdedor,
       pa.inicio, pa.competicion, pa.fronton, pa.eq1, pa.eq2, pa.estado, pa.puntos1, pa.puntos2,
       case
         when pa.estado <> 'jugado' or pa.puntos1 is null or pa.puntos2 is null then null
         when pr.ganador <> (case when pa.puntos1 > pa.puntos2 then 1 else 2 end) then 0
         else 3 + case
                    when pr.tantos_perdedor is null then 0
                    when pr.tantos_perdedor = least(pa.puntos1, pa.puntos2) then 3
                    when abs(pr.tantos_perdedor - least(pa.puntos1, pa.puntos2)) <= 2 then 1
                    else 0
                  end
       end as puntos
from public.porra_pronosticos pr
join public.porra_partidos pa on pa.id = pr.partido;
grant select on public.porra_puntuados to anon, authenticated;

-- Clasificación desde una fecha (null = toda la temporada)
create or replace function public.porra_clasificacion(desde timestamptz default null)
returns table (usuario uuid, alias text, puntos bigint, jugados bigint, aciertos bigint, exactos bigint)
language sql stable security invoker set search_path = '' as $$
  select pe.id, pe.alias,
         sum(pu.puntos), count(*), count(*) filter (where pu.puntos >= 3), count(*) filter (where pu.puntos = 6)
  from public.porra_puntuados pu
  join public.perfiles pe on pe.id = pu.usuario
  where pu.puntos is not null and (desde is null or pu.inicio >= desde)
  group by pe.id, pe.alias
  order by 3 desc, 5 desc, 6 desc, 4 asc, 2 asc
$$;
grant execute on function public.porra_clasificacion(timestamptz) to anon, authenticated;

-- Cuántos han elegido a cada equipo (se ve solo cuando el partido ha empezado)
create or replace function public.porra_reparto(ids text[])
returns table (partido text, ganador smallint, votos bigint)
language sql stable security invoker set search_path = '' as $$
  select partido, ganador, count(*) from public.porra_pronosticos
  where partido = any(ids) group by partido, ganador
$$;
grant execute on function public.porra_reparto(text[]) to anon, authenticated;

-- Borrar mi cuenta (perfil y pronósticos se borran en cascada)
create or replace function public.porra_borrar_cuenta()
returns void language sql security definer set search_path = '' as $$
  delete from auth.users where id = auth.uid();
$$;
revoke execute on function public.porra_borrar_cuenta() from public, anon;
grant execute on function public.porra_borrar_cuenta() to authenticated;
