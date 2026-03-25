/**
 * Shared SVG glyph markup used by the map UI and the feature explainer so icons stay identical.
 * Each fragment is intended inside viewBox="0 0 16 16" unless noted.
 */

/** Draw toolbar: stroke icons, parent svg should use fill="none" stroke="currentColor" stroke-width="1.3" */
export const drawToolGlyph = {
  point: '<circle cx="8" cy="6" r="3.5"/><path d="M8 9.5v4.5"/>',
  rectangle: '<rect x="2.5" y="3.5" width="11" height="9" rx="1"/>',
  polygon: '<path d="M8 2L14 6L12 14H4L2 6Z"/>',
};

/** Clear selection X (DrawTools); parent stroke-width="1.5" */
export const clearSelectionGlyph = '<path d="M4.5 4.5l7 7M11.5 4.5l-7 7"/>';

/** Map toolbar download; parent stroke-width="1.5" */
export const glyphDownloadMap = '<path d="M2 10v3h12v-3"/><path d="M8 2v8M5 7l3 3 3-3"/>';

/**
 * Map opacity moon; parent fill="none" stroke="currentColor" stroke-width="1.3".
 * Inner path uses fill (matches MapToolbar).
 */
export const glyphOpacityMoon =
  '<circle cx="8" cy="8" r="6"/><path d="M8 2a6 6 0 0 1 0 12" fill="currentColor" opacity="0.4"/>';

/** ZIP chevron; parent stroke-width="2" */
export const glyphZipChevron = '<path d="M6 2l6 6-6 6"/>';

/** Panel close; parent stroke-width="1.5" */
export const glyphPanelClose = '<path d="M4 4l8 8M12 4l-8 8"/>';
