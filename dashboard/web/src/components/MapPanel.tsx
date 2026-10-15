import { useEffect, useRef, useState } from "react";
import * as maplibregl from "maplibre-gl";
import type { Map, MapMouseEvent } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";

import { tileUrl } from "../api";
import type { Filters } from "../types";

type Props = {
  filters: Filters;
  onSelectField: (fieldId: string) => void;
};

const SOURCE_ID = "crop-fields";
const LAYER_IDS = ["crop-polygons", "crop-outlines", "crop-centroids"];
const BASEMAP_SOURCE_ID = "dashboard-basemap";
const BASEMAP_LAYER_ID = "dashboard-basemap";
maplibregl.setWorkerUrl(`${maplibreWorkerUrl}?v=20261015`);

type BasemapId =
  | "osm"
  | "opentopomap"
  | "esri-imagery"
  | "esri-streets"
  | "esri-topographic"
  | "none";

type BasemapOption = {
  id: BasemapId;
  label: string;
  enabled: boolean;
  requirement?: string;
};

const BASEMAP_OPTIONS: BasemapOption[] = [
  { id: "osm", label: "OpenStreetMap", enabled: true },
  { id: "opentopomap", label: "OpenTopoMap", enabled: true },
  {
    id: "esri-imagery",
    label: "Esri World Imagery",
    enabled: true,
  },
  {
    id: "esri-streets",
    label: "Esri World Street Map",
    enabled: true,
  },
  {
    id: "esri-topographic",
    label: "Esri World Topographic",
    enabled: true,
  },
  { id: "none", label: "Neutral background", enabled: true },
];
const labelColor: maplibregl.ExpressionSpecification = [
  "match",
  ["get", "label"],
  "maize",
  "#3f8f59",
  "beans",
  "#db8a35",
  "potatoes",
  "#367fba",
  "#9ba39e",
];

export function MapPanel({ filters, onSelectField }: Props) {
  const container = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const selectRef = useRef(onSelectField);
  const [mapReady, setMapReady] = useState(false);
  const [basemap, setBasemap] = useState<BasemapId>("osm");
  const [basemapError, setBasemapError] = useState<string | null>(null);
  selectRef.current = onSelectField;

  useEffect(() => {
    if (!container.current || mapRef.current) return;
    setMapReady(false);
    const map = new maplibregl.Map({
      container: container.current,
      center: [36.5, -0.4],
      zoom: 7.2,
      minZoom: 6,
      maxZoom: 18,
      attributionControl: false,
      style: {
        version: 8,
        sources: {},
        layers: [
          { id: "background", type: "background", paint: { "background-color": "#edf0e8" } },
        ],
      },
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.addControl(new maplibregl.AttributionControl({ compact: true }), "bottom-right");
    let active = true;
    const handleLoad = () => {
      if (active) setMapReady(true);
    };
    map.once("load", handleLoad);
    const queryCropFeatures = (event: MapMouseEvent) => {
      const availableLayers = LAYER_IDS.filter((layerId) => map.getLayer(layerId));
      if (!availableLayers.length) return [];
      return map.queryRenderedFeatures(event.point, { layers: availableLayers });
    };
    const handleClick = (event: MapMouseEvent) => {
      const features = queryCropFeatures(event);
      const fieldId = features[0]?.properties?.field_id as string | undefined;
      if (fieldId) selectRef.current(fieldId);
    };
    map.on("click", handleClick);
    map.on("mousemove", (event: MapMouseEvent) => {
      const features = queryCropFeatures(event);
      map.getCanvas().style.cursor = features.length ? "pointer" : "";
    });
    mapRef.current = map;
    return () => {
      active = false;
      map.off("load", handleLoad);
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    let disposed = false;

    const removeCurrentBasemap = () => {
      if (map.getLayer(BASEMAP_LAYER_ID)) map.removeLayer(BASEMAP_LAYER_ID);
      if (map.getSource(BASEMAP_SOURCE_ID)) map.removeSource(BASEMAP_SOURCE_ID);
    };

    const installBasemap = async () => {
      setBasemapError(null);
      removeCurrentBasemap();
      if (basemap === "none") return;

      try {
        let tiles: string[];
        let attribution: string;
        let rasterSaturation = 0;

        if (basemap === "osm") {
          tiles = ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"];
          attribution =
            '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>';
          rasterSaturation = -0.35;
        } else if (basemap === "opentopomap") {
          tiles = [
            "https://a.tile.opentopomap.org/{z}/{x}/{y}.png",
            "https://b.tile.opentopomap.org/{z}/{x}/{y}.png",
            "https://c.tile.opentopomap.org/{z}/{x}/{y}.png",
          ];
          attribution =
            'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>, SRTM | Map style &copy; <a href="https://opentopomap.org">OpenTopoMap</a> (CC-BY-SA)';
        } else if (basemap.startsWith("esri-")) {
          const service =
            basemap === "esri-imagery"
              ? "World_Imagery"
              : basemap === "esri-streets"
                ? "World_Street_Map"
                : "World_Topo_Map";
          tiles = [
            `https://server.arcgisonline.com/ArcGIS/rest/services/${service}/MapServer/tile/{z}/{y}/{x}`,
          ];
          attribution =
            basemap === "esri-imagery"
              ? "Source: Esri, Vantor, Earthstar Geographics, and the GIS User Community"
              : "Sources: Esri, HERE, Garmin, OpenStreetMap contributors, and the GIS User Community";
          if (basemap === "esri-imagery") rasterSaturation = -0.08;
        } else throw new Error("Unsupported basemap.");

        if (disposed) return;
        map.addSource(BASEMAP_SOURCE_ID, {
          type: "raster",
          tiles,
          tileSize: 256,
          attribution,
        });
        const firstOverlay = LAYER_IDS.find((layerId) => map.getLayer(layerId));
        map.addLayer(
          {
            id: BASEMAP_LAYER_ID,
            type: "raster",
            source: BASEMAP_SOURCE_ID,
            paint: { "raster-opacity": 0.78, "raster-saturation": rasterSaturation },
          },
          firstOverlay,
        );
      } catch (error) {
        if (!disposed) {
          setBasemapError(error instanceof Error ? error.message : "Basemap could not be loaded.");
        }
      }
    };

    void installBasemap();

    return () => {
      disposed = true;
    };
  }, [basemap, mapReady]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady || !filters.week) return;
    const installSource = () => {
      for (const layerId of LAYER_IDS) {
        if (map.getLayer(layerId)) map.removeLayer(layerId);
      }
      if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID);
      map.addSource(SOURCE_ID, {
        type: "vector",
        tiles: [tileUrl(filters)],
        minzoom: 6,
        maxzoom: 18,
      });
      map.addLayer({
        id: "crop-polygons",
        type: "fill",
        source: SOURCE_ID,
        "source-layer": "fields",
        minzoom: 11,
        paint: {
          "fill-color": labelColor,
          "fill-opacity": ["interpolate", ["linear"], ["zoom"], 11, 0.58, 16, 0.76],
        },
      });
      map.addLayer({
        id: "crop-outlines",
        type: "line",
        source: SOURCE_ID,
        "source-layer": "fields",
        minzoom: 11,
        paint: {
          "line-color": "#ffffff",
          "line-opacity": 0.72,
          "line-width": ["interpolate", ["linear"], ["zoom"], 11, 0.25, 17, 1.1],
        },
      });
      map.addLayer({
        id: "crop-centroids",
        type: "circle",
        source: SOURCE_ID,
        "source-layer": "fields",
        maxzoom: 11,
        paint: {
          "circle-color": labelColor,
          "circle-radius": [
            "interpolate",
            ["linear"],
            ["sqrt", ["get", "field_count"]],
            1,
            2,
            15,
            8,
          ],
          "circle-opacity": 0.72,
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 0.4,
        },
      });
    };
    installSource();
  }, [filters, mapReady]);

  return (
    <div className="map-shell">
      <div className="map-grid" />
      <div ref={container} className="map" aria-label="Crop prediction map" />
      <label className="basemap-picker">
        <span>Basemap</span>
        <select value={basemap} onChange={(event) => setBasemap(event.target.value as BasemapId)}>
          {BASEMAP_OPTIONS.map((option) => (
            <option key={option.id} value={option.id} disabled={!option.enabled}>
              {option.label}
              {!option.enabled && option.requirement ? ` — ${option.requirement}` : ""}
            </option>
          ))}
        </select>
        {basemapError && <small title={basemapError}>Provider unavailable</small>}
      </label>
      <div className="map-caption">Field predictions · click a field to inspect</div>
    </div>
  );
}
