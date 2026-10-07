-- ════════════════════════════════════════════════════════════════════
-- PORRA DE ESKUPILOTASTATS
-- Pegar entero en Supabase → SQL Editor → New query → Run.
-- Se puede volver a ejecutar sin perder datos (crea solo lo que falta y
-- rehace reglas, vistas y funciones).
--
-- Para hacerte administrador (elegir qué partidos entran en la general),
-- una vez creado tu alias ejecuta:
--   update public.perfiles set admin = true where alias = 'TuAlias';
-- ════════════════════════════════════════════════════════════════════

-- ── Perfiles: el alias público de cada usuario ──────────────────────
create table if not exists public.perfiles (
  id      uuid primary key references auth.users on delete cascade,
  alias   text not null check (char_length(alias) between 3 and 20
                               and alias ~ '^[A-Za-z0-9ÁÉÍÓÚÜÑáéíóúüñ _.-]+$'),
  oculto  boolean not null default false,          -- moderación: true lo saca de las clasificaciones
  creado  timestamptz not null default now()
);
alter table public.perfiles add column if not exists admin boolean not null default false;
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
alter table public.porra_partidos add column if not exists categoria text;   -- campeonato, torneo, desafio, festival
alter table public.porra_partidos add column if not exists activo boolean;   -- general: null = según el modo
create index if not exists porra_partidos_inicio on public.porra_partidos (inicio);

-- Qué partidos entran en la clasificación general (lo decide el administrador):
-- 'todos' | 'oficiales' (sin festivales) | 'manual' (solo los marcados)
create table if not exists public.porra_config (
  id   smallint primary key default 1 check (id = 1),
  modo text not null default 'oficiales' check (modo in ('todos', 'oficiales', 'manual'))
);
insert into public.porra_config (id) values (1) on conflict (id) do nothing;

-- ── Ligas privadas (hasta 20 personas, de una competición) ──────────
create table if not exists public.porra_ligas (
  id      uuid primary key default gen_random_uuid(),
  nombre  text not null check (char_length(nombre) between 3 and 40),
  codigo  text not null unique,                    -- para unirse: 6 letras/números
  creador uuid not null references public.perfiles (id) on delete cascade,
  alcance text[] not null check (cardinality(alcance) between 1 and 10),   -- competiciones que cuentan, o 'mes:2026-11'
  creada  timestamptz not null default now()
);
create table if not exists public.porra_miembros (
  liga    uuid not null references public.porra_ligas (id) on delete cascade,
  usuario uuid not null references public.perfiles (id) on delete cascade,
  unido   timestamptz not null default now(),
  primary key (liga, usuario)
);
create index if not exists porra_miembros_usuario on public.porra_miembros (usuario);

-- ── Pronósticos: uno por partido para la general (liga null) y otro por liga ─
create table if not exists public.porra_pronosticos (
  usuario         uuid not null default auth.uid() references public.perfiles (id) on delete cascade,
  partido         text not null references public.porra_partidos (id) on delete cascade,
  ganador         smallint not null check (ganador in (1, 2)),
  tantos_perdedor smallint check (tantos_perdedor between 0 and 21),
  actualizado     timestamptz not null default now()
);
alter table public.porra_pronosticos add column if not exists liga uuid references public.porra_ligas (id) on delete cascade;
do $$ begin
  -- La primera versión tenía la clave (usuario, partido): ahora va con la liga
  if exists (select 1 from pg_constraint where conname = 'porra_pronosticos_pkey') then
    alter table public.porra_pronosticos drop constraint porra_pronosticos_pkey;
  end if;
  if not exists (select 1 from pg_constraint where conname = 'porra_pronosticos_unico') then
    alter table public.porra_pronosticos
      add constraint porra_pronosticos_unico unique nulls not distinct (usuario, partido, liga);
  end if;
end $$;
create index if not exists porra_pronosticos_partido on public.porra_pronosticos (partido);

-- ── Funciones de apoyo para las reglas ──────────────────────────────
-- Partidos aún abiertos y si cuentan para la general. Se pronostica hasta
-- 1 hora antes del inicio de la velada (columna «cierre»).
drop view if exists public.porra_abiertos cascade;
create view public.porra_abiertos with (security_invoker = true) as
select p.id, p.inicio, p.inicio - interval '1 hour' as cierre, p.competicion, p.fase, p.fronton, p.modalidad, p.categoria,
       p.eq1, p.eq2, p.activo,
       coalesce(p.activo, case c.modo when 'todos' then true
                                      when 'oficiales' then coalesce(p.categoria, '') <> 'festival'
                                      else false end) as pronosticable
from public.porra_partidos p cross join public.porra_config c
where p.estado = 'abierto' and p.inicio - interval '1 hour' > now();

create or replace function public.porra_es_admin()
returns boolean language sql stable security definer set search_path = '' as $$
  select coalesce((select admin from public.perfiles where id = auth.uid()), false)
$$;

create or replace function public.porra_soy_miembro(lid uuid)
returns boolean language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.porra_miembros where liga = lid and usuario = auth.uid())
$$;

-- ¿Se puede pronosticar ahora este partido en este ámbito (null = general)?
drop policy if exists "pronosticos: crear"   on public.porra_pronosticos;
drop policy if exists "pronosticos: cambiar" on public.porra_pronosticos;
drop policy if exists "pronosticos: borrar"  on public.porra_pronosticos;
drop function if exists public.porra_pronosticable(text);
create or replace function public.porra_pronosticable(pid text, lid uuid default null)
returns boolean language sql stable security definer set search_path = '' as $$
  select case
    when lid is null then coalesce((select pronosticable from public.porra_abiertos where id = pid), false)
    else exists (select 1 from public.porra_abiertos a
                 join public.porra_ligas l on l.id = lid
                 join public.porra_miembros m on m.liga = l.id and m.usuario = auth.uid()
                 where a.id = pid
                   and (a.competicion = any(l.alcance)
                        or 'mes:' || to_char(a.inicio at time zone 'Europe/Madrid', 'YYYY-MM') = any(l.alcance)))
  end
$$;

-- ── Seguridad (Row Level Security) ──────────────────────────────────
alter table public.perfiles          enable row level security;
alter table public.porra_partidos    enable row level security;
alter table public.porra_pronosticos enable row level security;
alter table public.porra_config      enable row level security;
alter table public.porra_ligas       enable row level security;
alter table public.porra_miembros    enable row level security;

drop policy if exists "perfiles: ver"      on public.perfiles;
drop policy if exists "perfiles: crear"    on public.perfiles;
drop policy if exists "perfiles: cambiar"  on public.perfiles;
create policy "perfiles: ver"     on public.perfiles for select using (not oculto or id = auth.uid());
create policy "perfiles: crear"   on public.perfiles for insert to authenticated with check (id = auth.uid() and not oculto);
create policy "perfiles: cambiar" on public.perfiles for update to authenticated using (id = auth.uid()) with check (id = auth.uid());

drop policy if exists "partidos: ver"   on public.porra_partidos;
drop policy if exists "partidos: admin" on public.porra_partidos;
create policy "partidos: ver"   on public.porra_partidos for select using (true);
create policy "partidos: admin" on public.porra_partidos for update to authenticated
  using (public.porra_es_admin()) with check (public.porra_es_admin());

drop policy if exists "config: ver"   on public.porra_config;
drop policy if exists "config: admin" on public.porra_config;
create policy "config: ver"   on public.porra_config for select using (true);
create policy "config: admin" on public.porra_config for update to authenticated
  using (public.porra_es_admin()) with check (public.porra_es_admin());

-- Las ligas y sus miembros solo los ven sus miembros. Se crean y se entra
-- con las funciones de abajo; el creador puede borrarla y cada uno salirse.
drop policy if exists "ligas: ver"    on public.porra_ligas;
drop policy if exists "ligas: borrar" on public.porra_ligas;
create policy "ligas: ver"    on public.porra_ligas for select to authenticated using (public.porra_soy_miembro(id));
create policy "ligas: borrar" on public.porra_ligas for delete to authenticated using (creador = auth.uid());
drop policy if exists "miembros: ver"   on public.porra_miembros;
drop policy if exists "miembros: salir" on public.porra_miembros;
create policy "miembros: ver"   on public.porra_miembros for select to authenticated using (public.porra_soy_miembro(liga));
create policy "miembros: salir" on public.porra_miembros for delete to authenticated using (usuario = auth.uid());

-- Pronósticos: los ajenos solo se ven cuando se cierra el partido (y los de
-- una liga, solo sus miembros); se pronostica hasta 1 hora antes del inicio.
drop policy if exists "pronosticos: ver" on public.porra_pronosticos;
create policy "pronosticos: ver" on public.porra_pronosticos for select using (
  usuario = auth.uid()
  or (exists (select 1 from public.porra_partidos p where p.id = partido and p.inicio - interval '1 hour' <= now())
      and (liga is null or public.porra_soy_miembro(liga))));
create policy "pronosticos: crear" on public.porra_pronosticos for insert to authenticated with check (
  usuario = auth.uid() and public.porra_pronosticable(partido, liga));
create policy "pronosticos: cambiar" on public.porra_pronosticos for update to authenticated
  using (usuario = auth.uid())
  with check (usuario = auth.uid() and public.porra_pronosticable(partido, liga));
create policy "pronosticos: borrar" on public.porra_pronosticos for delete to authenticated using (
  usuario = auth.uid() and public.porra_pronosticable(partido, liga));

-- ── Permisos por columna ────────────────────────────────────────────
revoke all on public.perfiles, public.porra_partidos, public.porra_pronosticos, public.porra_config,
              public.porra_ligas, public.porra_miembros from anon, authenticated;
grant select on public.perfiles, public.porra_partidos, public.porra_pronosticos, public.porra_config,
               public.porra_abiertos to anon, authenticated;
grant select on public.porra_ligas, public.porra_miembros to anon;                -- sin reglas para anon: no ve nada
grant select, delete on public.porra_ligas, public.porra_miembros to authenticated;
grant insert (id, alias), update (alias) on public.perfiles to authenticated;      -- ni "admin" ni "oculto"
grant insert (partido, liga, ganador, tantos_perdedor), update (partido, liga, ganador, tantos_perdedor), delete
  on public.porra_pronosticos to authenticated;
grant update (activo) on public.porra_partidos to authenticated;                  -- solo admin (regla)
grant update (modo) on public.porra_config to authenticated;                      -- solo admin (regla)
grant all on public.perfiles, public.porra_partidos, public.porra_pronosticos, public.porra_config,
             public.porra_ligas, public.porra_miembros to service_role;
revoke execute on function public.porra_pronosticable(text, uuid), public.porra_es_admin() from public, anon;
grant execute on function public.porra_pronosticable(text, uuid), public.porra_es_admin() to authenticated;
revoke execute on function public.porra_soy_miembro(uuid) from public;
grant execute on function public.porra_soy_miembro(uuid) to anon, authenticated;    -- anon: siempre false

-- ── Ligas: crear, unirse ────────────────────────────────────────────
create or replace function public.porra_crear_liga(nombre text, alcance text[])
returns table (id uuid, codigo text) language plpgsql security definer set search_path = '' as $$
declare nueva uuid; cod text;
begin
  if not exists (select 1 from public.perfiles p where p.id = auth.uid()) then
    raise exception 'Primero elige tu alias';
  end if;
  if (select count(*) from public.porra_ligas l where l.creador = auth.uid()) >= 10 then
    raise exception 'Como mucho puedes crear 10 ligas';
  end if;
  loop
    -- 6 caracteres sin los que se confunden (0/O, 1/I/L)
    cod := (select string_agg(substr('ABCDEFGHJKMNPQRSTUVWXYZ23456789', 1 + floor(random() * 31)::int, 1), '')
            from generate_series(1, 6));
    exit when not exists (select 1 from public.porra_ligas l where l.codigo = cod);
  end loop;
  insert into public.porra_ligas (nombre, codigo, creador, alcance)
    values (trim(porra_crear_liga.nombre), cod, auth.uid(), porra_crear_liga.alcance) returning porra_ligas.id into nueva;
  insert into public.porra_miembros (liga, usuario) values (nueva, auth.uid());
  return query select nueva, cod;
end $$;

create or replace function public.porra_unirse(codigo text)
returns uuid language plpgsql security definer set search_path = '' as $$
declare lid uuid;
begin
  if not exists (select 1 from public.perfiles p where p.id = auth.uid()) then
    raise exception 'Primero elige tu alias';
  end if;
  select l.id into lid from public.porra_ligas l where l.codigo = upper(trim(porra_unirse.codigo)) for update;
  if lid is null then raise exception 'No existe ninguna liga con ese código'; end if;
  if exists (select 1 from public.porra_miembros m where m.liga = lid and m.usuario = auth.uid()) then return lid; end if;
  if (select count(*) from public.porra_miembros m where m.liga = lid) >= 20 then
    raise exception 'La liga está completa (20 personas)';
  end if;
  insert into public.porra_miembros (liga, usuario) values (lid, auth.uid());
  return lid;
end $$;
revoke execute on function public.porra_crear_liga(text, text[]), public.porra_unirse(text) from public, anon;
grant execute on function public.porra_crear_liga(text, text[]), public.porra_unirse(text) to authenticated;

-- ── Podios de los torneos individuales (mano a mano y 4 y medio) ────
-- Se pronostican campeón, subcampeón y los dos semifinalistas, hasta 1 hora
-- antes del primer partido del torneo. Los resultados los sube el workflow.
create table if not exists public.porra_podio_resultados (
  competicion text primary key,
  campeon     text not null,
  subcampeon  text not null,
  semis       text[] not null default '{}',              -- los que perdieron en semifinales
  actualizado timestamptz not null default now()
);
create table if not exists public.porra_podios (
  usuario     uuid not null default auth.uid() references public.perfiles (id) on delete cascade,
  competicion text not null,
  liga        uuid references public.porra_ligas (id) on delete cascade,   -- null = general
  campeon     text not null,
  subcampeon  text not null,
  semi1       text,
  semi2       text,
  actualizado timestamptz not null default now(),
  constraint porra_podios_unico unique nulls not distinct (usuario, competicion, liga),
  constraint porra_podios_distintos check (campeon <> subcampeon)
);

-- Nombres comparables: «P. Etxeberria» = «P.ETXEBERRIA»
create or replace function public.porra_clave(t text)
returns text language sql immutable set search_path = '' as $$
  select regexp_replace(lower(translate(coalesce(t, ''), 'ÁÉÍÓÚÜÑáéíóúüñ', 'AEIOUUNaeiouun')), '[^a-z0-9]', '', 'g')
$$;

-- Torneos con podio y su cierre (1 hora antes del primer partido subido)
drop view if exists public.porra_podio_torneos cascade;
create view public.porra_podio_torneos with (security_invoker = true) as
select competicion, min(inicio) - interval '1 hour' as cierre
from public.porra_partidos
where modalidad in ('mano', 'cuatro') and categoria in ('campeonato', 'torneo') and competicion is not null
group by competicion;

create or replace function public.porra_podio_abierto(comp text, lid uuid default null)
returns boolean language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.porra_podio_torneos t where t.competicion = comp and t.cierre > now())
     and (lid is null or exists (select 1 from public.porra_ligas l
                                 join public.porra_miembros m on m.liga = l.id and m.usuario = auth.uid()
                                 where l.id = lid and comp = any(l.alcance)))
$$;
create or replace function public.porra_podio_cerrado(comp text)
returns boolean language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.porra_podio_torneos t where t.competicion = comp and t.cierre <= now())
$$;

alter table public.porra_podios           enable row level security;
alter table public.porra_podio_resultados enable row level security;
drop policy if exists "podios: ver"     on public.porra_podios;
drop policy if exists "podios: crear"   on public.porra_podios;
drop policy if exists "podios: cambiar" on public.porra_podios;
drop policy if exists "podios: borrar"  on public.porra_podios;
create policy "podios: ver" on public.porra_podios for select using (
  usuario = auth.uid()
  or (public.porra_podio_cerrado(competicion) and (liga is null or public.porra_soy_miembro(liga))));
create policy "podios: crear" on public.porra_podios for insert to authenticated with check (
  usuario = auth.uid() and public.porra_podio_abierto(competicion, liga));
create policy "podios: cambiar" on public.porra_podios for update to authenticated
  using (usuario = auth.uid())
  with check (usuario = auth.uid() and public.porra_podio_abierto(competicion, liga));
create policy "podios: borrar" on public.porra_podios for delete to authenticated using (
  usuario = auth.uid() and public.porra_podio_abierto(competicion, liga));
drop policy if exists "podio resultados: ver" on public.porra_podio_resultados;
create policy "podio resultados: ver" on public.porra_podio_resultados for select using (true);

revoke all on public.porra_podios, public.porra_podio_resultados from anon, authenticated;
grant select on public.porra_podios, public.porra_podio_resultados, public.porra_podio_torneos to anon, authenticated;
grant insert (competicion, liga, campeon, subcampeon, semi1, semi2),
      update (competicion, liga, campeon, subcampeon, semi1, semi2), delete on public.porra_podios to authenticated;
grant all on public.porra_podios, public.porra_podio_resultados to service_role;
revoke execute on function public.porra_podio_abierto(text, uuid) from public, anon;
grant execute on function public.porra_podio_abierto(text, uuid) to authenticated;
revoke execute on function public.porra_podio_cerrado(text) from public;
grant execute on function public.porra_podio_cerrado(text) to anon, authenticated;

-- Puntos del podio: campeón 15, subcampeón 9, finalista en el puesto cambiado 5,
-- cada semifinalista 3 y +6 por el pleno (los cuatro en su sitio). Máximo 36.
drop view if exists public.porra_podios_puntuados;
create view public.porra_podios_puntuados with (security_invoker = true) as
select po.usuario, po.competicion, po.liga, po.campeon, po.subcampeon, po.semi1, po.semi2,
       r.campeon as real_campeon, r.subcampeon as real_subcampeon, r.semis as real_semis,
       case when r.competicion is null then null else
           (case when k.c = k.rc then 15 when k.c = k.rs then 5 else 0 end)
         + (case when k.s = k.rs then 9 when k.s = k.rc then 5 else 0 end)
         + (case when k.s1 <> '' and k.s1 = any(k.rsemis) then 3 else 0 end)
         + (case when k.s2 <> '' and k.s2 <> k.s1 and k.s2 = any(k.rsemis) then 3 else 0 end)
         + (case when k.c = k.rc and k.s = k.rs and k.s1 <> k.s2
                  and k.s1 = any(k.rsemis) and k.s2 = any(k.rsemis) then 6 else 0 end)
       end as puntos
from public.porra_podios po
left join public.porra_podio_resultados r on r.competicion = po.competicion
cross join lateral (select public.porra_clave(po.campeon) as c, public.porra_clave(po.subcampeon) as s,
                           public.porra_clave(po.semi1) as s1, public.porra_clave(po.semi2) as s2,
                           public.porra_clave(r.campeon) as rc, public.porra_clave(r.subcampeon) as rs,
                           (select coalesce(array_agg(public.porra_clave(x)), '{}') from unnest(r.semis) x) as rsemis) k;
grant select on public.porra_podios_puntuados to anon, authenticated;

-- ── Puntuación ──────────────────────────────────────────────────────
-- 3 puntos por acertar el ganador; si además se acierta el tanteo del
-- perdedor, +3 (exacto) o +1 (a 2 tantos o menos). Máximo 6.
drop function if exists public.porra_clasificacion(timestamptz);
drop function if exists public.porra_clasificacion(timestamptz, uuid);
drop function if exists public.porra_clasificacion(timestamptz, uuid, text);
drop function if exists public.porra_clasificacion(uuid, text[], text);
drop view if exists public.porra_puntuados;
create view public.porra_puntuados with (security_invoker = true) as
select pr.usuario, pr.partido, pr.liga, pr.ganador, pr.tantos_perdedor,
       pa.inicio, pa.competicion, pa.fronton, pa.eq1, pa.eq2, pa.estado, pa.puntos1, pa.puntos2, pa.categoria,
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

-- Empates a puntos: los deshacen los puntos en partidos oficiales (sin festivales).
-- Clasificación de la general (liga null) o de una liga; en la general, de unas
-- competiciones (un torneo: serie A, B o las dos) o de un mes ('2026-11', hora de
-- España). En una liga salen todos sus miembros, aunque aún no tengan puntos.
create or replace function public.porra_clasificacion(liga uuid default null, competiciones text[] default null,
                                                      mes text default null)
returns table (usuario uuid, alias text, puntos bigint, oficiales bigint, jugados bigint, aciertos bigint, exactos bigint)
language sql stable security invoker set search_path = '' as $$
  with partidos as (
    select pu.usuario, sum(pu.puntos) as pts,
           coalesce(sum(pu.puntos) filter (where coalesce(pu.categoria, '') <> 'festival'), 0) as ofi,   -- desempate
           count(*) as jug, count(*) filter (where pu.puntos >= 3) as aci, count(*) filter (where pu.puntos = 6) as exa
    from public.porra_puntuados pu
    where pu.liga is not distinct from porra_clasificacion.liga and pu.puntos is not null
      and (porra_clasificacion.competiciones is null or pu.competicion = any(porra_clasificacion.competiciones))
      and (porra_clasificacion.mes is null
           or to_char(pu.inicio at time zone 'Europe/Madrid', 'YYYY-MM') = porra_clasificacion.mes)
    group by pu.usuario
  ), podios as (
    -- El podio suma en la porra de su torneo y en las ligas, no en los meses
    select pp.usuario, sum(pp.puntos) as pts
    from public.porra_podios_puntuados pp
    where pp.puntos is not null and pp.liga is not distinct from porra_clasificacion.liga
      and porra_clasificacion.mes is null
      and (porra_clasificacion.liga is not null or porra_clasificacion.competiciones is not null)
      and (porra_clasificacion.competiciones is null or pp.competicion = any(porra_clasificacion.competiciones))
    group by pp.usuario
  )
  select pe.id, pe.alias,
         coalesce(pa.pts, 0) + coalesce(po.pts, 0), coalesce(pa.ofi, 0) + coalesce(po.pts, 0),
         coalesce(pa.jug, 0), coalesce(pa.aci, 0), coalesce(pa.exa, 0)
  from public.perfiles pe
  left join partidos pa on pa.usuario = pe.id
  left join podios po on po.usuario = pe.id
  where case when porra_clasificacion.liga is null then pa.usuario is not null or po.usuario is not null
             else exists (select 1 from public.porra_miembros m
                          where m.liga = porra_clasificacion.liga and m.usuario = pe.id) end
  order by 3 desc, 4 desc, 6 desc, 7 desc, 5 asc, 2 asc
$$;
grant execute on function public.porra_clasificacion(uuid, text[], text) to anon, authenticated;

-- Ranking anual: al cerrarse cada mes, los 50 primeros de la general de ese mes
-- reciben de 50 a 1 puntos. Los empates se deshacen con los puntos en partidos
-- oficiales; si aún empatan, reciben los mismos. Solo cuentan meses cerrados.
drop function if exists public.porra_ranking_anual(int);
create or replace function public.porra_ranking_anual(anio int)
returns table (usuario uuid, alias text, puntos bigint, meses bigint, ganados bigint, mejor int)
language sql stable security invoker set search_path = '' as $$
  with mensual as (
    select pu.usuario, to_char(pu.inicio at time zone 'Europe/Madrid', 'YYYY-MM') as mes, sum(pu.puntos) as pts,
           coalesce(sum(pu.puntos) filter (where coalesce(pu.categoria, '') <> 'festival'), 0) as ofi
    from public.porra_puntuados pu
    join public.perfiles pe on pe.id = pu.usuario                -- sin los perfiles ocultos
    where pu.liga is null and pu.puntos is not null
      and extract(year from pu.inicio at time zone 'Europe/Madrid') = anio
      and date_trunc('month', pu.inicio at time zone 'Europe/Madrid') < date_trunc('month', now() at time zone 'Europe/Madrid')
    group by 1, 2
  ), puestos as (
    select usuario, mes, rank() over (partition by mes order by pts desc, ofi desc) as puesto from mensual
  )
  select pe.id, pe.alias, sum(greatest(0, 51 - p.puesto))::bigint, count(*), count(*) filter (where p.puesto = 1),
         min(p.puesto)::int
  from puestos p join public.perfiles pe on pe.id = p.usuario
  group by pe.id, pe.alias
  order by 3 desc, 5 desc, 6 asc, 2 asc
$$;
grant execute on function public.porra_ranking_anual(int) to anon, authenticated;

-- Cuántos han elegido a cada equipo en la general (cuando el partido ha empezado)
create or replace function public.porra_reparto(ids text[])
returns table (partido text, ganador smallint, votos bigint)
language sql stable security invoker set search_path = '' as $$
  select partido, ganador, count(*) from public.porra_pronosticos
  where partido = any(ids) and liga is null group by partido, ganador
$$;
grant execute on function public.porra_reparto(text[]) to anon, authenticated;

-- Borrar mi cuenta (perfil, pronósticos, ligas creadas y membresías en cascada)
create or replace function public.porra_borrar_cuenta()
returns void language sql security definer set search_path = '' as $$
  delete from auth.users where id = auth.uid();
$$;
revoke execute on function public.porra_borrar_cuenta() from public, anon;
grant execute on function public.porra_borrar_cuenta() to authenticated;
