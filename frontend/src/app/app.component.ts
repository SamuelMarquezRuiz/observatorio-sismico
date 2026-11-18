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
  readonly selectedMode = signal<'live' | 'replay'>('live');
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
  private timer?: ReturnType<typeof setTimeout>;
  private controller = new AbortController();
  private destroyed = false;
  private currentConfig?: Config;

  async onMapReady(map: L.Map): Promise<void> {
    this.map = map;
    const base = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      noWrap: true,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);

    const borders = L.geoJSON(undefined, {
      style: { color: '#334155', weight: 1, fill: false },
      onEachFeature: (feature, layer) => {
        const name = document.createElement('span');
        name.textContent = feature.properties?.NAME_ES ?? feature.properties?.ADMIN ?? 'Sin nombre';
        layer.bindTooltip(name);
      }
    }).addTo(map);

    this.eventsLayer.addTo(map);
    L.control.layers({ 'OpenStreetMap': base }, { 'Países (Natural Earth)': borders, 'Terremotos': this.eventsLayer }).addTo(map);
    map.attributionControl.addAttribution('Fronteras: Natural Earth · Datos reales: USGS');

    // Cargar polígonos de países locales
    try {
      borders.addData(await this.readJson<FeatureCollection>('datos/paises.geojson'));
      this.countriesStatus.set('Catálogo de países cargado');
    } catch {
      this.countriesStatus.set('No se pudo cargar el catálogo de países');
    }

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

      // 2. Construir query string de filtros
      const params = new URLSearchParams();
      params.set('mode', this.selectedMode());
      if (this.minMagFilter()) params.set('min_mag', this.minMagFilter());
      if (this.countryFilter()) params.set('country_id', this.countryFilter());
      if (this.startDateFilter()) params.set('start', new Date(this.startDateFilter()).toISOString());
      if (this.endDateFilter()) params.set('end', new Date(this.endDateFilter()).toISOString());

      const urlEventos = `${baseApi}/api/eventos?${params.toString()}`;
      const urlEstadisticas = `${baseApi}/api/estadisticas?${params.toString()}`;

      // 3. Consultar eventos y estadísticas en paralelo
      const [dataEventos, dataStats] = await Promise.all([
        this.readJson<EventsResponse>(urlEventos, headers),
        this.readJson<EstadisticasResponse>(urlEstadisticas, headers).catch(() => null)
      ]);

      if (this.destroyed) return;

      this.isReplayMode.set(this.selectedMode() === 'replay');
      this.apiConnected.set(true);
      this.status.set(this.selectedMode() === 'replay' ? 'MODO REPRODUCCIÓN HISTÓRICA' : 'API conectada · datos en streaming');

      if (dataEventos && Array.isArray(dataEventos.features)) {
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
      if (!this.destroyed && cfg.mode === 'api') {
        clearTimeout(this.timer);
        this.timer = setTimeout(
          () => void this.refresh(cfg),
          Math.max(30000, Number(cfg.refreshMs) || 60000)
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

      L.circleMarker([lat, lon], {
        radius: magnitude === null ? 4 : Math.max(4, Math.min(22, 3 + (magnitude + 1) * 2.2)),
        color,
        fillColor: color,
        fillOpacity: 0.75,
        weight: 1
      }).bindPopup(popup).addTo(next);

      count++;
    }

    this.eventsLayer.clearLayers();
    this.eventsLayer.addLayer(next);
    this.count.set(count);
    this.omitted.set(omitted);
  }

  // Métodos de control de filtros
  onModeChange(event: Event): void {
    const val = (event.target as HTMLSelectElement).value as 'live' | 'replay';
    this.selectedMode.set(val);
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
