import { test, expect } from '@playwright/test';
import { TestHelpers } from './utils/test-helpers';
import { testModels } from './utils/fixtures/test-data';

test.describe('Model Management Tests', () => {
  let helpers: TestHelpers;

  test.beforeEach(async ({ page }) => {
    helpers = new TestHelpers(page);
    await page.goto('/');
    await helpers.waitForAppLoad();
    await helpers.navigateToSection('models');
  });

  test('should display models section', async ({ page }) => {
    await expect(page.locator('.models-section')).toBeVisible();
    await expect(page.locator('.model-list-panel')).toBeVisible();
    await expect(page.locator('.model-detail-panel')).toBeVisible();
  });

  test('should open model creation form', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();
    await helpers.expectFormToBeVisible();
    await expect(page.getByPlaceholder('モデル名を入力')).toBeVisible();
    await expect(page.getByPlaceholder('パスを入力')).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'SQL' })).toBeVisible();
  });

  test('should close model creation form', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();
    await helpers.expectFormToBeVisible();

    await helpers.closeForm();
    await helpers.expectFormToBeHidden();
  });

  test('should create simple model', async ({ page }) => {
    const { name, path, sql } = testModels.simple;

    await page.getByRole('button', { name: '新規作成' }).click();
    await page.getByPlaceholder('モデル名を入力').fill(name);
    await page.getByPlaceholder('パスを入力').fill(path);
    await page.getByRole('textbox', { name: 'SQL' }).fill(sql);

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(1000);

    await helpers.expectItemInList(name);
  });

  test('should create aggregation model', async ({ page }) => {
    const { name, path, sql } = testModels.aggregation;

    await page.getByRole('button', { name: '新規作成' }).click();
    await page.getByPlaceholder('モデル名を入力').fill(name);
    await page.getByPlaceholder('パスを入力').fill(path);
    await page.getByRole('textbox', { name: 'SQL' }).fill(sql);

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(1000);

    await helpers.expectItemInList(name);
  });

  test('should display model details', async ({ page }) => {
    const { name, path, sql } = testModels.simple;

    await helpers.createModel(name, path, sql);

    await page.getByText(name).click();
    await page.waitForTimeout(500);

    await expect(page.locator('.model-detail-panel')).toContainText(name);
    await expect(page.locator('.model-detail-panel')).toContainText(path);
    await expect(page.locator('.model-detail-panel')).toContainText('SELECT 1');
  });

  test('should edit model', async ({ page }) => {
    const { name, path, sql } = testModels.simple;
    const updatedSql = 'SELECT 2 as updated_column';

    await helpers.createModel(name, path, sql);
    await page.getByText(name).click();
    await page.waitForTimeout(500);

    await page.getByRole('button', { name: '編集' }).click();
    await helpers.expectFormToBeVisible();

    await page.getByRole('textbox', { name: 'SQL' }).clear();
    await page.getByRole('textbox', { name: 'SQL' }).fill(updatedSql);

    await page.getByRole('button', { name: '更新' }).click();
    await page.waitForTimeout(1000);

    await expect(page.locator('.model-detail-panel')).toContainText('SELECT 2');
  });

  test('should delete model with confirmation', async ({ page }) => {
    const { name, path, sql } = testModels.simple;

    await helpers.createModel(name, path, sql);
    await helpers.expectItemInList(name);

    await helpers.deleteItem(name);
    await helpers.expectItemNotInList(name);
  });

  test('should handle multiple models', async ({ page }) => {
    const simple = testModels.simple;
    const transformation = testModels.transformation;

    await helpers.createModel(simple.name, simple.path, simple.sql);
    await helpers.createModel(
      transformation.name,
      transformation.path,
      transformation.sql,
    );

    await helpers.expectItemInList(simple.name);
    await helpers.expectItemInList(transformation.name);

    await page.getByText(simple.name).click();
    await expect(page.locator('.model-detail-panel')).toContainText(
      simple.name,
    );

    await page.getByText(transformation.name).click();
    await expect(page.locator('.model-detail-panel')).toContainText(
      transformation.name,
    );
  });

  test('should show run button for models', async ({ page }) => {
    const { name, path, sql } = testModels.simple;

    await helpers.createModel(name, path, sql);
    await page.getByText(name).click();
    await page.waitForTimeout(500);

    await expect(page.getByRole('button', { name: '実行' })).toBeVisible();
  });

  test('should validate required fields', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(500);

    await helpers.expectFormToBeVisible();
  });
});
