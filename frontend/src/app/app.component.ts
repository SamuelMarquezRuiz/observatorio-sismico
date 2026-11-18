import { Component, OnDestroy, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { LeafletDirective } from '@bluehalo/ngx-leaflet';
import * as L from 'leaflet';
import type { FeatureCollection, Point } from 'geojson';

export interface EarthquakeProperty {
  mag: number | null;
  magType?: string | null;
  place?: string | null;
  time: number;
  updated: number;
  country_id?: string | null;
  country_name?: string | null;
}

export interface EarthquakeFeature {
  type: 'Feature';
  id: string;
  geometry: {
    type: 'Point';
    coordinates: [number, number, number?];
  };
  properties: EarthquakeProperty;
}

export interface ApiResponseMetadata {
  generated?: number;
  last_ingestion?: number;
  mode?: string;
  count?: number;
  total_coincidentes?: number;
  truncated?: boolean;
}

export interface EventsResponse {
  type: 'FeatureCollection';
  metadata?: ApiResponseMetadata;
  features: EarthquakeFeature[];
}

export interface Config {
  mode: 'demo' | 'api';
  apiUrl: string;
  refreshMs: number;
  skipNgrokWarning: boolean;
}

export interface PaisItem {
  country_id: string;
  name: string;
  continent?: string;
}

export interface HourlyWindow {
  hora_utc: string;
  timestamp: number;
  count: number;
  mag_media: number | null;
  mag_min: number | null;
  mag_max: number | null;
}

export interface EstadisticasResponse {
  metadata?: {
    generated?: number;
    mode?: string;
    temporal_granularity?: 'hour' | 'day';
  };
  total_eventos: number;
  resumen_metricas?: {
    total_con_magnitud: number;
    total_magnitud_nula: number;
    mag_media: number | null;
    mag_min: number | null;
    mag_max: number | null;
    profundidad_media_km: number | null;
  };
  por_ventana_hora: HourlyWindow[];
  por_pais: Array<{ country_id: string | null; country_name: string; count: number; mag_media: number | null }>;
  distribucion_magnitud: Record<string, number>;
  distribucion_profundidad: Record<string, number>;
  comparacion_historica?: {
    criterio: string;
    dias_periodo_historico: number;
    total_historico_registrado: number;
    media_diaria_historica: number;
    recuento_reciente_24h: number;
    diferencia: number;
    variacion_porcentual: number | string;
    interpretacion: string;
  };
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, LeafletDirective],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnDestroy {
  protected readonly Math = Math;

  // Estado general
  readonly mode = signal('Cargando configuración...');
  readonly status = signal('Preparando el mapa...');
  readonly apiConnected = signal(true);
  readonly isReplayMode = signal(false);
  readonly ingestionAlert = signal(false);

  // Metadatos temporales
  readonly lastRead = signal('Sin lectura');
  readonly generated = signal('Sin información');
  readonly lastIngestion = signal('Desconocida');
  readonly count = signal(0);
  readonly omitted = signal(0);
  readonly countriesStatus = signal('Cargando catálogo');

  // Filtros interactivos
  readonly selectedMode = signal<'live' | 'replay' | 'historical'>('live');
  readonly minMagFilter = signal<string>('');
  readonly countryFilter = signal<string>('');
  readonly startDateFilter = signal<string>('');
  readonly endDateFilter = signal<string>('');

  // Datos
  readonly listaPaises = signal<PaisItem[]>([]);
  readonly eventosList = signal<EarthquakeFeature[]>([]);
  readonly estadisticas = signal<EstadisticasResponse | null>(null);

  // Paginación de la tabla
  readonly currentPage = signal(1);
  readonly pageSize = signal(10);

  readonly totalPages = computed(() => {
    const total = this.eventosList().length;
    return Math.max(1, Math.ceil(total / this.pageSize()));
  });

  readonly paginatedEvents = computed(() => {
    const page = this.currentPage();
    const size = this.pageSize();
    const start = (page - 1) * size;
    return this.eventosList().slice(start, start + size);
  });

  readonly maxHourlyCount = computed(() => {
    const windows = this.estadisticas()?.por_ventana_hora ?? [];
    if (windows.length === 0) return 1;
    return Math.max(1, ...windows.map(w => w.count));
  });

  // Selector mensual y desglose diario completo (sin scroll horizontal)
  private readonly MESES_ES = [
    'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
  ];

  readonly selectedChartMonth = signal<string>('');

  readonly isDailyChartMode = computed(() => {
    return this.estadisticas()?.metadata?.temporal_granularity === 'day';
  });

  readonly availableMonths = computed(() => {
    const windows = this.estadisticas()?.por_ventana_hora ?? [];
    const monthsSet = new Set<string>();
    for (const w of windows) {
      if (w.hora_utc && w.hora_utc.length >= 7) {
        monthsSet.add(w.hora_utc.substring(0, 7));
      }
    }
    if (monthsSet.size === 0) {
      for (const ev of this.eventosList()) {
        if (ev.properties?.time) {
          monthsSet.add(new Date(ev.properties.time).toISOString().substring(0, 7));
        }
      }
    }
    return Array.from(monthsSet).sort().map(ym => {
      const [year, monthStr] = ym.split('-');
      const monthIdx = parseInt(monthStr, 10) - 1;
      return {
        value: ym,
        label: `${this.MESES_ES[monthIdx] ?? monthStr} ${year}`
      };
    });
  });

  readonly activeChartMonth = computed(() => {
    const current = this.selectedChartMonth();
    const avail = this.availableMonths();
    if (avail.length === 0) return '';
    if (current && avail.some(m => m.value === current)) {
      return current;
    }
    return avail[0].value;
  });

  readonly dailyChartData = computed(() => {
    const ym = this.activeChartMonth();
    if (!ym) return [];
    const [yearStr, monthStr] = ym.split('-');
    const year = parseInt(yearStr, 10);
    const month = parseInt(monthStr, 10);
    const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();

    const countsByDay = new Map<string, number>();
    for (const w of this.estadisticas()?.por_ventana_hora ?? []) {
      if (w.hora_utc && w.hora_utc.startsWith(ym)) {
        const dayKey = w.hora_utc.substring(0, 10);
        countsByDay.set(dayKey, (countsByDay.get(dayKey) ?? 0) + w.count);
      }
    }

    const result: Array<{ date: string; dayLabel: string; count: number }> = [];
    for (let d = 1; d <= daysInMonth; d++) {
      const dayPadded = String(d).padStart(2, '0');
      const dateKey = `${ym}-${dayPadded}`;
      result.push({
        date: dateKey,
        dayLabel: dayPadded,
        count: countsByDay.get(dateKey) ?? 0
      });
    }
    return result;
  });

  readonly maxDailyCount = computed(() => {
    const days = this.dailyChartData();
    if (days.length === 0) return 1;
    return Math.max(1, ...days.map(d => d.count));
  });

  readonly monthlyTotalEvents = computed(() => {
    return this.dailyChartData().reduce((acc, d) => acc + d.count, 0);
  });

  // Auto-refresco (30s) y seguimiento de nuevos eventos en streaming
  readonly autoRefreshEnabled = signal(true);
  readonly newEventIds = signal<Set<string>>(new Set());

  // Evento destacado (mayor magnitud) de la selección actual
  readonly featuredEvent = computed<EarthquakeFeature | null>(() => {
    const list = this.eventosList();
    let best: EarthquakeFeature | null = null;
    let bestMag = -Infinity;
    for (const ev of list) {
      const m = ev.properties?.mag;
      if (typeof m === 'number' && Number.isFinite(m) && m > bestMag) {
        bestMag = m;
        best = ev;
      }
    }
    return best;
  });

  // Top 5 regiones/países con mayor actividad sísmica calculado dinámicamente
  readonly topRegions = computed(() => {
    const list = this.eventosList();
    if (list.length === 0) return [];
    const counts = new Map<string, number>();
    for (const ev of list) {
      const region = ev.properties?.country_name || 'Sin país asignado';
      counts.set(region, (counts.get(region) ?? 0) + 1);
    }
    return Array.from(counts.entries())
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 5);
  });

  // Configuración de mapa
  readonly options: L.MapOptions = {
    center: L.latLng(18, 0),
    zoom: 2,
    minZoom: 2,
    maxZoom: 8,
    preferCanvas: true,
    worldCopyJump: false
  };

  private map?: L.Map;
  private eventsLayer = L.layerGroup();
  private markerMap = new Map<string, L.CircleMarker>();
  private knownEventIds = new Set<string>();
  private hasLoadedOnce = false;
  private timer?: ReturnType<typeof setTimeout>;
  private controller = new AbortController();
  private destroyed = false;
  private currentConfig?: Config;

  async onMapReady(map: L.Map): Promise<void> {
    this.map = map;

    const esriDark = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 16,
      noWrap: true,
      attribution: 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ'
    }).addTo(map);

    const esriOcean = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 13,
      noWrap: true,
      attribution: 'Tiles &copy; Esri &mdash; Sources: GEBCO, NOAA, CHS, OSU, UNH, CSUMB, National Geographic, DeLorme, and others'
    });

    const esriPhysical = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Physical_Map/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 8,
      noWrap: true,
      attribution: 'Tiles &copy; Esri &mdash; Source: US National Park Service'
    });

    const openTopo = L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
      maxZoom: 17,
      noWrap: true,
      attribution: 'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, <a href="http://viewfinderpanoramas.org">SRTM</a> | Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a> (<a href="https://creativecommons.org/licenses/by-sa/3.0/">CC-BY-SA</a>)'
    });

    const esriSat = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 19,
      noWrap: true,
      attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
    });

    const osmStandard = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      noWrap: true,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    });

    const borders = L.geoJSON(undefined, {
      style: { color: '#475569', weight: 1, fill: false },
      onEachFeature: (feature, layer) => {
        const name = document.createElement('span');
        name.textContent = feature.properties?.NAME_ES ?? feature.properties?.ADMIN ?? 'Sin nombre';
        layer.bindTooltip(name);
      }
    }).addTo(map);

    const tectonicPlates = L.geoJSON(undefined, {
      style: { color: '#f97316', weight: 1.5, opacity: 0.6, dashArray: '4 4' }
    }).addTo(map);

    this.eventsLayer.addTo(map);

    const baseMaps: Record<string, L.TileLayer> = {
      'Modo Oscuro (Esri)': esriDark,
      'Océanos y Batimetría (Esri)': esriOcean,
      'Relieve Físico (Esri)': esriPhysical,
      'Topográfico (OpenTopoMap)': openTopo,
      'Vista Satélite (Esri)': esriSat,
      'Mapa Estándar (OSM)': osmStandard
    };

    const overlayMaps: Record<string, L.Layer> = {
      'Placas Tectónicas (PB2002)': tectonicPlates,
      'Fronteras (Natural Earth)': borders,
      'Terremotos': this.eventsLayer
    };

    L.control.layers(baseMaps, overlayMaps, { position: 'topright' }).addTo(map);
    map.attributionControl.addAttribution('Fronteras: Natural Earth · Placas: PB2002 · Datos reales: USGS');

    // Cargar polígonos de países locales
    try {
      borders.addData(await this.readJson<FeatureCollection>('datos/paises.geojson'));
      this.countriesStatus.set('Catálogo de países cargado');
    } catch {
      this.countriesStatus.set('No se pudo cargar el catálogo de países');
    }

    // Cargar límites de placas tectónicas en segundo plano (capa opcional)
    void this.readJson<FeatureCollection>(
      'https://raw.githubusercontent.com/fraxen/tectonicplates/master/GeoJSON/PB2002_boundaries.json'
    ).then(plates => {
      if (!this.destroyed) {
        tectonicPlates.addData(plates);
      }
    }).catch(() => {
      // Silencioso si no hay conexión externa
    });

    if (this.destroyed) return;

    // Leer config.json
    try {
      const config = await this.readJson<Config>('config.json');
      if (!['demo', 'api'].includes(config.mode)) throw new Error('Modo inválido');
      this.currentConfig = config;
      this.mode.set(config.mode === 'demo' ? 'DEMOSTRACIÓN · datos ficticios' : 'API · backend en tiempo real');
      await this.refresh(config);
      if (config.mode === 'api') {
        this.cargarCatalogoPaises(config);
      }
    } catch (e) {
      this.status.set('Error: revisa public/config.json');
      this.apiConnected.set(false);
    }
  }

  private async readJson<T>(url: string, headers: Record<string, string> = {}): Promise<T> {
    const response = await fetch(url, {
      headers,
      cache: 'no-store',
      signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(15000)])
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json() as Promise<T>;
  }

  async refresh(config?: Config): Promise<void> {
    const cfg = config || this.currentConfig;
    if (!cfg) return;

    try {
      const headers: Record<string, string> = {};
      if (cfg.mode === 'api' && cfg.skipNgrokWarning) {
        headers['ngrok-skip-browser-warning'] = '1';
      }

      if (cfg.mode === 'demo') {
        const data = await this.readJson<EventsResponse>('datos/terremotos-demo.geojson');
        this.renderEvents(data.features || []);
        this.status.set('Ejemplo estático: no representa actividad real');
        this.apiConnected.set(true);
        this.isReplayMode.set(false);
        return;
      }

      // 1. Consultar /api/salud
      const baseApi = cfg.apiUrl.replace(/\/$/, '');
      try {
        const salud = await this.readJson<any>(`${baseApi}/api/salud`, headers);
        this.apiConnected.set(salud.status === 'ok');
        this.ingestionAlert.set(salud.estado_ingesta === 'desactualizada');
        if (salud.last_ingestion_utc) {
          this.lastIngestion.set(new Date(salud.last_ingestion_utc).toLocaleString('es-ES', { timeZone: 'UTC' }) + ' UTC');
        }
      } catch {
        this.apiConnected.set(false);
      }

      // 2. Construir query string de filtros sin truncamientos artificiales
      const params = new URLSearchParams();
      params.set('mode', this.selectedMode());
      if (this.minMagFilter()) params.set('min_mag', this.minMagFilter());
      if (this.countryFilter()) params.set('country_id', this.countryFilter());
      if (this.startDateFilter()) params.set('start', `${this.startDateFilter()}T00:00:00.000Z`);
      if (this.endDateFilter()) params.set('end', `${this.endDateFilter()}T23:59:59.999Z`);

      const eventParams = new URLSearchParams(params);
      eventParams.set('limit', '25000');

      const urlEventos = `${baseApi}/api/eventos?${eventParams.toString()}`;
      const urlEstadisticas = `${baseApi}/api/estadisticas?${params.toString()}`;

      // 3. Consultar eventos y estadísticas en paralelo
      const [dataEventos, dataStats] = await Promise.all([
        this.readJson<EventsResponse>(urlEventos, headers),
        this.readJson<EstadisticasResponse>(urlEstadisticas, headers).catch(() => null)
      ]);

      if (this.destroyed) return;

      this.isReplayMode.set(this.selectedMode() === 'replay');
      this.apiConnected.set(true);
      this.status.set(
        this.selectedMode() === 'replay'
          ? 'MODO REPRODUCCIÓN HISTÓRICA'
          : this.selectedMode() === 'historical'
            ? 'CATÁLOGO HISTÓRICO 2025 · datos consolidados'
            : 'API conectada · datos en streaming'
      );

      if (dataEventos && Array.isArray(dataEventos.features)) {
        // Detectar eventos recién llegados en streaming (después de la carga inicial)
        if (this.selectedMode() === 'live') {
          if (this.hasLoadedOnce) {
            const newlyArrived = new Set<string>();
            for (const f of dataEventos.features) {
              if (!this.knownEventIds.has(f.id)) {
                newlyArrived.add(f.id);
              }
            }
            this.newEventIds.set(newlyArrived);
          } else {
            this.newEventIds.set(new Set());
          }
          this.knownEventIds = new Set(dataEventos.features.map(f => f.id));
          this.hasLoadedOnce = true;
        } else {
          this.newEventIds.set(new Set());
        }

        this.renderEvents(dataEventos.features);
        this.eventosList.set(dataEventos.features);
        this.currentPage.set(1);
      }

      if (dataStats) {
        this.estadisticas.set(dataStats);
      }

      this.lastRead.set(new Date().toLocaleTimeString('es-ES', { timeZone: 'UTC' }) + ' UTC');
      if (dataEventos.metadata?.generated) {
        this.generated.set(new Date(dataEventos.metadata.generated).toLocaleTimeString('es-ES', { timeZone: 'UTC' }) + ' UTC');
      }

    } catch (err) {
      loggerError: if (!this.destroyed) {
        this.apiConnected.set(false);
        this.status.set('No se pudo conectar a la API: se conservan los datos previos. Revisa túnel ngrok y CORS.');
      }
    } finally {
      clearTimeout(this.timer);
      if (!this.destroyed && cfg.mode === 'api' && this.selectedMode() !== 'historical' && this.autoRefreshEnabled()) {
        // El enunciado fija 60 s: evita refrescos innecesarios (cuotas de ngrok/Netlify)
        this.timer = setTimeout(
          () => void this.refresh(cfg),
          Math.max(60000, Number(cfg.refreshMs) || 60000)
        );
      }
    }
  }

  private async cargarCatalogoPaises(config: Config): Promise<void> {
    try {
      const baseApi = config.apiUrl.replace(/\/$/, '');
      const headers: Record<string, string> = config.skipNgrokWarning ? { 'ngrok-skip-browser-warning': '1' } : {};
      const paises = await this.readJson<PaisItem[]>(`${baseApi}/api/paises`, headers);
      if (Array.isArray(paises)) {
        this.listaPaises.set(paises);
      }
    } catch {
      // Si falla, se conservará la lista vacía sin romper el mapa
    }
  }

  private renderEvents(features: EarthquakeFeature[]): void {
    const next = L.layerGroup();
    this.markerMap.clear();
    let omitted = 0;
    let count = 0;

    for (const feature of features) {
      const p = feature.properties;
      const c = feature.geometry?.type === 'Point' ? feature.geometry.coordinates : [];
      const [lon, lat, depth] = c;

      if (!p || !Number.isFinite(lon) || !Number.isFinite(lat) || Math.abs(lon) > 180 || Math.abs(lat) > 90) {
        omitted++;
        continue;
      }

      const magnitude = typeof p.mag === 'number' && Number.isFinite(p.mag) ? p.mag : null;
      const depthOk = typeof depth === 'number' && Number.isFinite(depth);
      const color = !depthOk ? '#64748b' : depth < 70 ? '#e8a317' : depth < 300 ? '#e35b37' : '#7b3fb6';

      const popup = document.createElement('div');
      popup.style.lineHeight = '1.4';
      popup.innerHTML = `
        <strong>${this.escapeHtml(p.place ?? 'Sin descripción')}</strong><br>
        <b>Magnitud:</b> ${magnitude !== null ? magnitude.toFixed(1) + ' (' + (p.magType ?? 'ND') + ')' : 'ND'}<br>
        <b>Profundidad:</b> ${depthOk ? depth + ' km' : 'ND'}<br>
        <b>Fecha UTC:</b> ${this.formatDate(p.time)}<br>
        <b>País:</b> ${this.escapeHtml(p.country_name ?? 'Sin país asignado')}<br>
        <b>ID:</b> <code>${this.escapeHtml(feature.id)}</code>
      `;

      const marker = L.circleMarker([lat, lon], {
        radius: magnitude === null ? 4 : Math.max(4, Math.min(22, 3 + (magnitude + 1) * 2.2)),
        color,
        fillColor: color,
        fillOpacity: 0.75,
        weight: 1
      }).bindPopup(popup).addTo(next);

      this.markerMap.set(feature.id, marker);
      count++;
    }

    this.eventsLayer.clearLayers();
    this.eventsLayer.addLayer(next);
    this.count.set(count);
    this.omitted.set(omitted);
  }

  // Interactividad Mapa <-> Tabla y utilidades
  focusOnEvent(ev: EarthquakeFeature): void {
    const c = ev.geometry?.coordinates;
    if (!c || c.length < 2) return;
    const [lon, lat] = c;
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) return;

    document.getElementById('map-section')?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    this.map?.flyTo([lat, lon], 6, { duration: 1.0 });

    const marker = this.markerMap.get(ev.id);
    if (marker) {
      setTimeout(() => {
        marker.openPopup();
      }, 450);
    }
  }

  exportCsv(): void {
    const list = this.eventosList();
    if (list.length === 0) return;

    const escapeCsv = (val: unknown): string => {
      const str = val === null || val === undefined ? '' : String(val);
      return `"${str.replace(/"/g, '""')}"`;
    };

    const headers = ['event_id', 'fecha_utc', 'magnitud', 'tipo_magnitud', 'profundidad_km', 'latitud', 'longitud', 'pais', 'ubicacion'];
    const rows = list.map(ev => {
      const p = ev.properties;
      const [lon, lat, depth] = ev.geometry.coordinates;
      return [
        escapeCsv(ev.id),
        escapeCsv(this.formatDate(p.time)),
        escapeCsv(p.mag ?? 'ND'),
        escapeCsv(p.magType ?? ''),
        escapeCsv(depth ?? 'ND'),
        escapeCsv(lat),
        escapeCsv(lon),
        escapeCsv(p.country_name ?? 'Sin país asignado'),
        escapeCsv(p.place ?? '')
      ].join(',');
    });

    const csvContent = '\uFEFF' + [headers.join(','), ...rows].join('\r\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `terremotos-${this.selectedMode()}-${new Date().toISOString().slice(0, 10)}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }

  toggleAutoRefresh(): void {
    const nextState = !this.autoRefreshEnabled();
    this.autoRefreshEnabled.set(nextState);
    clearTimeout(this.timer);
    if (nextState && this.selectedMode() !== 'historical' && this.currentConfig) {
      this.timer = setTimeout(() => void this.refresh(this.currentConfig), 60000);
    }
  }

  // Métodos de control de filtros
  onModeChange(event: Event): void {
    const val = (event.target as HTMLSelectElement).value as 'live' | 'replay' | 'historical';
    this.selectedMode.set(val);
    this.hasLoadedOnce = false;
    this.knownEventIds.clear();
    this.newEventIds.set(new Set());
    this.refresh();
  }

  onMinMagInput(event: Event): void {
    this.minMagFilter.set((event.target as HTMLInputElement).value);
  }

  onCountryChange(event: Event): void {
    this.countryFilter.set((event.target as HTMLSelectElement).value);
  }

  onStartDateChange(event: Event): void {
    this.startDateFilter.set((event.target as HTMLInputElement).value);
  }

  onEndDateChange(event: Event): void {
    this.endDateFilter.set((event.target as HTMLInputElement).value);
  }

  onChartMonthChange(event: Event): void {
    this.selectedChartMonth.set((event.target as HTMLSelectElement).value);
  }

  applyFilters(): void {
    this.refresh();
  }

  resetFilters(): void {
    this.minMagFilter.set('');
    this.countryFilter.set('');
    this.startDateFilter.set('');
    this.endDateFilter.set('');
    this.selectedMode.set('live');
    this.refresh();
  }

  // Paginación
  prevPage(): void {
    if (this.currentPage() > 1) {
      this.currentPage.update(p => p - 1);
    }
  }

  nextPage(): void {
    if (this.currentPage() < this.totalPages()) {
      this.currentPage.update(p => p + 1);
    }
  }

  // Helpers de formateo
  formatDate(ms: number | null | undefined): string {
    if (!ms || typeof ms !== 'number') return 'ND';
    const d = new Date(ms);
    return isNaN(d.getTime()) ? 'ND' : d.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
  }

  formatHour(iso: string): string {
    if (!iso) return '';
    return iso.substring(11, 16);
  }

  formatWindowLabel(iso: string, granularity?: string): string {
    if (!iso) return '';
    if (granularity === 'day') {
      // YYYY-MM-DD -> DD/MM
      return `${iso.substring(8, 10)}/${iso.substring(5, 7)}`;
    }
    return iso.substring(11, 16);
  }

  getPercentage(count: number, total: number): number {
    if (!total || total === 0) return 0;
    return Math.round((count / total) * 100);
  }

  private escapeHtml(str: string): string {
    return str.replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]!));
  }

  ngOnDestroy(): void {
    this.destroyed = true;
    clearTimeout(this.timer);
    this.controller.abort();
  }
}
