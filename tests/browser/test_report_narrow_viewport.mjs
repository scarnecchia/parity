import { chromium } from 'playwright';
import { pathToFileURL } from 'node:url';
import path from 'node:path';

const fixture = path.resolve('tests/browser/report-narrow-viewport.html');
const url = pathToFileURL(fixture).href;
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  for (const width of [375, 1280]) {
    await page.setViewportSize({ width, height: 800 });
    await page.goto(url);
    const result = await page.evaluate(() => {
      const region = document.querySelector('[role="region"][tabindex="0"]');
      const bounds = region?.getBoundingClientRect();
      return {
        viewportWidth: window.innerWidth,
        documentWidth: document.documentElement.scrollWidth,
        headingVisible: Boolean(document.querySelector('h1')?.getBoundingClientRect().width),
        linkVisible: Boolean(document.querySelector('a')?.getBoundingClientRect().width),
        omissionVisible: document.body.innerText.includes('1 omitted (row_limit)'),
        regionFocusable: region?.getAttribute('tabindex') === '0',
        regionScrollable: Boolean(bounds && region.scrollWidth > region.clientWidth),
      };
    });
    if (result.documentWidth > width) throw new Error(`document overflow at ${width}px: ${JSON.stringify(result)}`);
    if (!result.headingVisible || !result.linkVisible || !result.omissionVisible || !result.regionFocusable || (width === 375 && !result.regionScrollable)) {
      throw new Error(`narrow viewport contract failed at ${width}px: ${JSON.stringify(result)}`);
    }
    let focused = null;
    for (let attempt = 0; attempt < 6 && focused !== 'region'; attempt += 1) {
      await page.keyboard.press('Tab');
      focused = await page.evaluate(() => document.activeElement?.getAttribute('role'));
    }
    if (focused !== 'region') throw new Error(`scroll region is not keyboard-focusable: ${focused}`);
    console.log(JSON.stringify({ width, ...result, focusedRegion: focused }));
  }
} finally {
  await browser.close();
}
