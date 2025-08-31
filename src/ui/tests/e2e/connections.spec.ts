import { test, expect } from '@playwright/test';
import { TestHelpers } from './utils/test-helpers';
import { testConnections } from './utils/fixtures/test-data';

test.describe('Connection Management Tests', () => {
  let helpers: TestHelpers;

  test.beforeEach(async ({ page }) => {
    helpers = new TestHelpers(page);
    await page.goto('/');
    await helpers.waitForAppLoad();
    await helpers.navigateToSection('connections');
  });

  test('should display connections list', async ({ page }) => {
    await expect(page.locator('.connection-list-panel')).toBeVisible();
    await expect(page.locator('.connection-detail-panel')).toBeVisible();
  });

  test('should open connection creation form', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();
    await helpers.expectFormToBeVisible();
    await expect(page.getByPlaceholder('接続名を入力')).toBeVisible();
  });

  test('should close connection creation form', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();
    await helpers.expectFormToBeVisible();

    await helpers.closeForm();
    await helpers.expectFormToBeHidden();
  });

  test('should create sqlite connection', async ({ page }) => {
    const { name, type, config } = testConnections.sqlite;

    await page.getByRole('button', { name: '新規作成' }).click();
    await page.getByPlaceholder('接続名を入力').fill(name);
    await page.getByRole('combobox').selectOption(type);
    await page.getByPlaceholder('path').fill(config.path);

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(1000);

    await helpers.expectItemInList(name);
  });

  test('should display connection details', async ({ page }) => {
    const { name, type, config } = testConnections.sqlite;

    await helpers.createConnection(name, type, config);

    await page.getByText(name).click();
    await page.waitForTimeout(500);

    await expect(page.locator('.connection-detail-panel')).toContainText(name);
    await expect(page.locator('.connection-detail-panel')).toContainText(type);
  });

  test('should edit connection', async ({ page }) => {
    const { name, type, config } = testConnections.sqlite;
    const updatedPath = './updated_test.db';

    await helpers.createConnection(name, type, config);
    await page.getByText(name).click();
    await page.waitForTimeout(500);

    await page.getByRole('button', { name: '編集' }).click();
    await helpers.expectFormToBeVisible();

    await page.getByPlaceholder('path').clear();
    await page.getByPlaceholder('path').fill(updatedPath);

    await page.getByRole('button', { name: '更新' }).click();
    await page.waitForTimeout(1000);

    await expect(page.locator('.connection-detail-panel')).toContainText(
      updatedPath,
    );
  });

  test('should delete connection with confirmation', async ({ page }) => {
    const { name, type, config } = testConnections.sqlite;

    await helpers.createConnection(name, type, config);
    await helpers.expectItemInList(name);

    await helpers.deleteItem(name);
    await helpers.expectItemNotInList(name);
  });

  test('should handle multiple connections', async ({ page }) => {
    const sqlite = testConnections.sqlite;
    const mysql = testConnections.mysql;

    await helpers.createConnection(sqlite.name, sqlite.type, sqlite.config);
    await helpers.createConnection(mysql.name, mysql.type, mysql.config);

    await helpers.expectItemInList(sqlite.name);
    await helpers.expectItemInList(mysql.name);

    await page.getByText(sqlite.name).click();
    await expect(page.locator('.connection-detail-panel')).toContainText(
      sqlite.name,
    );

    await page.getByText(mysql.name).click();
    await expect(page.locator('.connection-detail-panel')).toContainText(
      mysql.name,
    );
  });

  test('should validate required fields', async ({ page }) => {
    await page.getByRole('button', { name: '新規作成' }).click();

    await page.getByRole('button', { name: '作成' }).click();
    await page.waitForTimeout(500);

    await helpers.expectFormToBeVisible();
  });
});
