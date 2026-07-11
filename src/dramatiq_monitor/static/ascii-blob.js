/* AsciiBlob — procedural ASCII art that follows the cursor.
 *
 * Renders an animated character blob into any element with [data-ascii-blob].
 * Pure vanilla JS, no dependencies. Honors prefers-reduced-motion by rendering
 * a single static frame instead of animating.
 */
(function () {
  "use strict";

  var COLS = 72;
  var ROWS = 24;
  var RAMP = " .'`^\",:;Il!i~+_-?][}{1)(|\\/tfjrxnuvczXYUJCLQ0OZmwqpdbkhao*#MW&8%B@$";

  function init(el) {
    var target = { x: COLS / 2, y: ROWS / 2 };
    var eased = { x: COLS / 2, y: ROWS / 2 };
    var ripples = [];
    var inside = false;

    function render(now) {
      var t = now / 1000;
      var out = "";
      for (var y = 0; y < ROWS; y++) {
        for (var x = 0; x < COLS; x++) {
          var dx = (x - COLS / 2) / (COLS / 2);
          var dy = (y - ROWS / 2) / (ROWS / 2);
          var d = Math.sqrt(dx * dx * 1.15 + dy * dy * 1.35);
          var j = Math.max(0, 1 - d);
          if (j <= 0.02) { out += " "; continue; }
          var wave = (0.5 * Math.sin(0.36 * y + 0.9 * t) + 0.5 * Math.cos(0.26 * x - 0.7 * t) + 1) * 0.07;
          var cdx = x - eased.x;
          var cdy = (y - eased.y) * 2.1;
          var f = inside ? Math.max(0, 1 - Math.sqrt(cdx * cdx + cdy * cdy) / 14) * 0.55 : 0;
          for (var i = 0; i < ripples.length; i++) {
            var rp = ripples[i];
            var age = (now - rp.start) / 900;
            var rr = age * 30;
            var rd = Math.abs(Math.sqrt((x - rp.x) * (x - rp.x) + ((y - rp.y) * 2.1) * ((y - rp.y) * 2.1)) - rr);
            f += Math.max(0, 1 - rd / 2.5) * (1 - age) * 0.8;
          }
          var k = Math.min(1, (0.12 + wave + f) * j);
          out += RAMP[Math.min(RAMP.length - 1, Math.floor(k * (RAMP.length - 1)))];
        }
        if (y < ROWS - 1) out += "\n";
      }
      return out;
    }

    if (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      el.textContent = render(0);
      return;
    }

    window.addEventListener("mousemove", function (e) {
      var r = el.getBoundingClientRect();
      target.x = ((e.clientX - r.left) / r.width) * COLS;
      target.y = ((e.clientY - r.top) / r.height) * ROWS;
      inside = e.clientX >= r.left - 40 && e.clientX <= r.right + 40 &&
               e.clientY >= r.top - 40 && e.clientY <= r.bottom + 40;
    });
    el.addEventListener("click", function () {
      ripples.push({ start: performance.now(), x: eased.x, y: eased.y });
    });

    function loop(now) {
      eased.x += (target.x - eased.x) * 0.12;
      eased.y += (target.y - eased.y) * 0.12;
      ripples = ripples.filter(function (r) { return now - r.start < 900; });
      el.textContent = render(now);
      requestAnimationFrame(loop);
    }
    requestAnimationFrame(loop);
  }

  document.addEventListener("DOMContentLoaded", function () {
    var els = document.querySelectorAll("[data-ascii-blob]");
    for (var i = 0; i < els.length; i++) init(els[i]);
  });
})();
