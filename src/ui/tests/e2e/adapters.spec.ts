import { test, expect } from '@playwright/test';
import { TestHelpers } from './utils/test-helpers';
import { testAdapters } from './utils/fixtures/test-data';

test.describe('Adapter Management Tests', () => {
  let helpers: TestHelpers;

  test.beforeEach(async ({ page }) => {
    helpers = new TestHelpers(page);
    await page.goto('/');
    await helpers.waitForAppLoad();
    await helpers.navigateToSection('adapters');
  });

  test('should display adapters section', async ({ page }) => {
    await expect(page.locator('.adapters-section')).toBeVisible();
    await expect(page.locator('.adapter-list-panel')).toBeVisible();
    await expect(page.locator('.adapter-detail-panel')).toBeVisible();
  });

  test('should open adapter creation form', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();
    await helpers.expectFormToBeVisible();
    await expect(page.getByPlaceholder('アダプター名を入力')).toBeVisible();
  });

  test('should close adapter creation form', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();
    await helpers.expectFormToBeVisible();

    await helpers.closeForm();
    await helpers.expectFormToBeHidden();
  });

  test('should create csv adapter', async ({ page }) => {
    const { name, type, config } = testAdapters.csv;

    await page.getByRole('button', { name: '新規作成' }).click();
    await page.getByPlaceholder('アダプター名を入力').fill(name);
    await page.getByRole('combobox').selectOption(type);
    await page.getByPlaceholder('path').fill(config.path);
    await page.getByPlaceholder('delimiter').fill(config.delimiter);
    await page.getByPlaceholder('header').fill(config.header);

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(1000);

    await helpers.expectItemInList(name);
  });

  test('should create json adapter', async ({ page }) => {
    const { name, type, config } = testAdapters.json;

    await page.getByRole('button', { name: '新規作成' }).click();
    await page.getByPlaceholder('アダプター名を入力').fill(name);
    await page.getByRole('combobox').selectOption(type);
    await page.getByPlaceholder('path').fill(config.path);

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(1000);

    await helpers.expectItemInList(name);
  });

  test('should display adapter details', async ({ page }) => {
    const { name, type, config } = testAdapters.csv;

    await helpers.createAdapter(name, type, config);

    await page.getByText(name).click();
    await page.waitForTimeout(500);

    await expect(page.locator('.adapter-detail-panel')).toContainText(name);
    await expect(page.locator('.adapter-detail-panel')).toContainText(type);
  });

  test('should edit adapter', async ({ page }) => {
    const { name, type, config } = testAdapters.csv;
    const updatedPath = './updated_data.csv';

    await helpers.createAdapter(name, type, config);
    await page.getByText(name).click();
    await page.waitForTimeout(500);

    await page.getByRole('button', { name: '編集' }).click();
    await helpers.expectFormToBeVisible();

    await page.getByPlaceholder('path').clear();
    await page.getByPlaceholder('path').fill(updatedPath);

    await page.getByRole('button', { name: '更新' }).click();
    await page.waitForTimeout(1000);

    await expect(page.locator('.adapter-detail-panel')).toContainText(
      updatedPath,
    );
  });

  test('should delete adapter with confirmation', async ({ page }) => {
    const { name, type, config } = testAdapters.csv;

    await helpers.createAdapter(name, type, config);
    await helpers.expectItemInList(name);

    await helpers.deleteItem(name);
    await helpers.expectItemNotInList(name);
  });

  test('should handle multiple adapters', async ({ page }) => {
    const csv = testAdapters.csv;
    const json = testAdapters.json;

    await helpers.createAdapter(csv.name, csv.type, csv.config);
    await helpers.createAdapter(json.name, json.type, json.config);

    await helpers.expectItemInList(csv.name);
    await helpers.expectItemInList(json.name);

    await page.getByText(csv.name).click();
    await expect(page.locator('.adapter-detail-panel')).toContainText(csv.name);

    await page.getByText(json.name).click();
    await expect(page.locator('.adapter-detail-panel')).toContainText(
      json.name,
    );
  });

  test('should validate required fields', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(500);

    await helpers.expectFormToBeVisible();
  });

  test('should handle different adapter types', async ({ page }) => {
    const adapters = [
      testAdapters.csv,
      testAdapters.json,
      testAdapters.parquet,
    ];

    for (const adapter of adapters) {
      await helpers.createAdapter(adapter.name, adapter.type, adapter.config);
      await helpers.expectItemInList(adapter.name);
    }

    for (const adapter of adapters) {
      await page.getByText(adapter.name).click();
      await expect(page.locator('.adapter-detail-panel')).toContainText(
        adapter.type,
      );
    }
  });
});
