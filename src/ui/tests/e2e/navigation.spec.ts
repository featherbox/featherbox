import { test, expect } from '@playwright/test';
import { TestHelpers } from './utils/test-helpers';

test.describe('Navigation Tests', () => {
  let helpers: TestHelpers;

  test.beforeEach(async ({ page }) => {
    helpers = new TestHelpers(page);
    await page.goto('/');
    await helpers.waitForAppLoad();
  });

  test('should display navigation menu on load', async ({ page }) => {
    await expect(page.locator('nav.navigation')).toBeVisible();
    await expect(page.locator('.nav-items')).toBeVisible();
  });

  test('should start with connections section active', async ({ page }) => {
    await helpers.expectActiveSectionToBe('connections');
    await expect(page.locator('.connections-section')).toBeVisible();
  });

  test('should navigate to adapters section', async ({ page }) => {
    await helpers.navigateToSection('adapters');
    await helpers.expectActiveSectionToBe('adapters');
    await expect(page.locator('.adapters-section')).toBeVisible();
  });

  test('should navigate to models section', async ({ page }) => {
    await helpers.navigateToSection('models');
    await helpers.expectActiveSectionToBe('models');
    await expect(page.locator('.models-section')).toBeVisible();
  });

  test('should navigate to analysis section', async ({ page }) => {
    await helpers.navigateToSection('analysis');
    await helpers.expectActiveSectionToBe('analysis');
    await expect(page.locator('.placeholder')).toBeVisible();
    await expect(page.getByText('Analysis')).toBeVisible();
  });

  test('should navigate to settings section', async ({ page }) => {
    await helpers.navigateToSection('settings');
    await helpers.expectActiveSectionToBe('settings');
    await expect(page.locator('.placeholder')).toBeVisible();
    await expect(page.getByText('Settings')).toBeVisible();
  });

  test('should switch between sections multiple times', async ({ page }) => {
    await helpers.navigateToSection('adapters');
    await helpers.expectActiveSectionToBe('adapters');

    await helpers.navigateToSection('models');
    await helpers.expectActiveSectionToBe('models');

    await helpers.navigateToSection('connections');
    await helpers.expectActiveSectionToBe('connections');

    await helpers.navigateToSection('settings');
    await helpers.expectActiveSectionToBe('settings');
  });

  test('should show active visual indicator', async ({ page }) => {
    const connectionsButton = page.locator('.nav-item').first();
    await expect(connectionsButton).toHaveClass(/active/);

    await helpers.navigateToSection('adapters');
    const adaptersButton = page.locator('.nav-item').nth(1);
    await expect(adaptersButton).toHaveClass(/active/);
    await expect(connectionsButton).not.toHaveClass(/active/);
  });
});
