/**
 * ``use:fitText`` — a text element must never spill out of its bubble.
 *
 * The element keeps its stylesheet size while the text fits. When it would
 * overflow its own box (nowrap text wider than the pill, or a line taller than
 * a fixed-height head), the font shrinks in half-pixel steps down to ``min``.
 * If it still does not fit at ``min`` the element's own ``overflow: hidden``
 * and ``text-overflow: ellipsis`` take over — the text is cut, never drawn
 * outside the bubble. Re-fits on content changes and on resize of the element
 * or its parent.
 *
 * Usage: ``<span class="name" use:fitText>{title}</span>`` or
 * ``use:fitText={{ min: 11 }}``. The element needs a bounded width (a block,
 * a flex child with ``min-width: 0``, or ``max-width``) for overflow to be
 * measurable; the action also sets ``overflow: hidden`` so a miss is clipped.
 */
export function fitText(node, opts = {}) {
  let min = opts.min ?? 10;
  const step = 0.5;
  let fitting = false;

  const overflows = () => node.scrollWidth > node.clientWidth + 1 || node.scrollHeight > node.clientHeight + 1;

  function fit() {
    if (fitting || !node.isConnected) return;
    fitting = true;
    try {
      node.style.fontSize = '';
      if (!overflows()) return;
      node.style.overflow = 'hidden';
      let size = parseFloat(getComputedStyle(node).fontSize) || 14;
      while (overflows() && size - step >= min) {
        size -= step;
        node.style.fontSize = `${size}px`;
      }
    } finally {
      fitting = false;
    }
  }

  const ro = typeof ResizeObserver === 'function' ? new ResizeObserver(() => fit()) : null;
  ro?.observe(node);
  if (node.parentElement) ro?.observe(node.parentElement);
  const mo = typeof MutationObserver === 'function'
    ? new MutationObserver(() => fit()) : null;
  mo?.observe(node, { childList: true, characterData: true, subtree: true });
  fit();
  // Fonts loading after mount change metrics; refit once they are in.
  document.fonts?.ready?.then(() => fit()).catch(() => {});

  return {
    update(next = {}) { min = next.min ?? min; fit(); },
    destroy() { ro?.disconnect(); mo?.disconnect(); },
  };
}
