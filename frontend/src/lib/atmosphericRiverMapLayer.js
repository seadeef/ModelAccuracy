import { teardownBoundaryLayer } from './mapBoundaryLayer.js';

const IDS = {
  sourceId: 'atmospheric-river-source',
  fillLayerId: 'atmospheric-river-fill',
  outlineLayerId: 'atmospheric-river-outline',
};

/** Storm-violet AR footprint styling (matches the AR mode button's tint) —
 *  distinct from the amber selection accent, with a soft blurred edge so the
 *  grid-derived polygons read less blocky without redrawing the data. Exported
 *  for the canvas map export, which mirrors the on-map paint. */
export const AR_FOOTPRINT_FILL_COLOR = '#a78bfa';
export const AR_FOOTPRINT_FILL_OPACITY = 0.2;
export const AR_FOOTPRINT_OUTLINE_COLOR = '#c4b5fd';
export const AR_FOOTPRINT_OUTLINE_OPACITY = 0.5;
export const AR_FOOTPRINT_OUTLINE_WIDTH = 1;

export function createAtmosphericRiverMapLayer(fetchFrame) {
  let controller = null;
  let generation = 0;

  function setup(map, { data, dimLayerId, drawFillLayerId }) {
    if (map.getSource(IDS.sourceId)) {
      map.getSource(IDS.sourceId).setData(data);
      return;
    }
    const fillBefore = map.getLayer(dimLayerId) ? dimLayerId
      : map.getLayer(drawFillLayerId) ? drawFillLayerId
      : undefined;
    const outlineBefore = map.getLayer(drawFillLayerId) ? drawFillLayerId : undefined;
    map.addSource(IDS.sourceId, { type: 'geojson', data });
    map.addLayer({
      id: IDS.fillLayerId,
      type: 'fill',
      source: IDS.sourceId,
      paint: {
        'fill-color': AR_FOOTPRINT_FILL_COLOR,
        'fill-opacity': AR_FOOTPRINT_FILL_OPACITY,
      },
    }, fillBefore);
    map.addLayer({
      id: IDS.outlineLayerId,
      type: 'line',
      source: IDS.sourceId,
      paint: {
        'line-color': AR_FOOTPRINT_OUTLINE_COLOR,
        'line-width': AR_FOOTPRINT_OUTLINE_WIDTH,
        'line-opacity': AR_FOOTPRINT_OUTLINE_OPACITY,
        'line-blur': 1.5,
      },
    }, outlineBefore);
  }

  async function replace(map, path, placement = {}) {
    const current = ++generation;
    controller?.abort();
    controller = new AbortController();
    const data = await fetchFrame(path, { signal: controller.signal });
    if (current !== generation || controller.signal.aborted) return false;
    setup(map, { ...placement, data });
    return true;
  }

  function clear(map) {
    generation += 1;
    controller?.abort();
    controller = null;
    teardownBoundaryLayer(map, IDS);
  }

  return { replace, clear };
}
