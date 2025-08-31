import { Page, expect } from '@playwright/test';

export class TestHelpers {
  constructor(private page: Page) {}

  async waitForAppLoad() {
    await this.page.waitForSelector('nav.navigation');
    await this.page.waitForSelector('main.main-content');
  }

  async navigateToSection(
    section: 'connections' | 'adapters' | 'models' | 'analysis' | 'settings',
  ) {
    const sectionButton = this.page.getByRole('button', { name: section });
    await sectionButton.click();
    await this.page.waitForTimeout(100);
  }

  async expectActiveSectionToBe(section: string) {
    const activeNavItem = this.page.locator('.nav-item.active');
    await expect(activeNavItem).toBeVisible();

    const sectionMap = {
      connections: 'Connection',
      adapters: 'Database',
      models: 'FileText',
      analysis: 'BarChart3',
      settings: 'Settings',
    };

    const expectedIcon = sectionMap[section as keyof typeof sectionMap];
    if (expectedIcon) {
      await expect(
        activeNavItem.locator(`[data-lucide="${expectedIcon.toLowerCase()}"]`),
      ).toBeVisible();
    }
  }

  async createConnection(
    name: string,
    type: string = 'sqlite',
    config: any = {},
  ) {
    await this.page.getByRole('button', { name: '新規作成' }).click();

    await this.page.getByPlaceholder('接続名を入力').fill(name);
    await this.page.getByRole('combobox').selectOption(type);

    for (const [key, value] of Object.entries(config)) {
      await this.page.getByPlaceholder(key).fill(value as string);
    }

    await this.page.getByRole('button', { name: '作成' }).click();
    await this.page.waitForTimeout(500);
  }

  async createAdapter(name: string, type: string = 'csv', config: any = {}) {
    await this.page.getByRole('button', { name: '新規作成' }).click();

    await this.page.getByPlaceholder('アダプター名を入力').fill(name);
    await this.page.getByRole('combobox').selectOption(type);

    for (const [key, value] of Object.entries(config)) {
      await this.page.getByPlaceholder(key).fill(value as string);
    }

    await this.page.getByRole('button', { name: '作成' }).click();
    await this.page.waitForTimeout(500);
  }

  async createModel(name: string, path: string, sql: string = 'SELECT 1') {
    await this.page.getByRole('button', { name: '新規作成' }).click();

    await this.page.getByPlaceholder('モデル名を入力').fill(name);
    await this.page.getByPlaceholder('パスを入力').fill(path);
    await this.page.getByRole('textbox', { name: 'SQL' }).fill(sql);

    await this.page.getByRole('button', { name: '作成' }).click();
    await this.page.waitForTimeout(500);
  }

  async deleteItem(itemName: string) {
    await this.page.getByText(itemName).click();
    await this.page.getByRole('button', { name: '削除' }).click();

    await this.page.once('dialog', (dialog) => dialog.accept());
    await this.page.waitForTimeout(500);
  }

  async expectItemInList(itemName: string) {
    await expect(this.page.getByText(itemName)).toBeVisible();
  }

  async expectItemNotInList(itemName: string) {
    await expect(this.page.getByText(itemName)).not.toBeVisible();
  }

  async expectFormToBeVisible() {
    await expect(this.page.locator('.form-overlay')).toBeVisible();
  }

  async expectFormToBeHidden() {
    await expect(this.page.locator('.form-overlay')).not.toBeVisible();
  }

  async closeForm() {
    await this.page.getByRole('button', { name: 'キャンセル' }).click();
    await this.page.waitForTimeout(100);
  }

  async setupFreshProject() {
    const projectName = `test_project_${Date.now()}`;

    await this.page.evaluate(async (name) => {
      const response = await fetch('http://localhost:3000/api/test/setup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ projectName: name }),
      });
      if (!response.ok) {
        throw new Error(`Setup failed: ${response.statusText}`);
      }
    }, projectName);

    return projectName;
  }

  async cleanupProject(projectName: string) {
    await this.page.evaluate(async (name) => {
      const response = await fetch('http://localhost:3000/api/test/cleanup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ projectName: name }),
      });
      if (!response.ok) {
        console.warn(`Cleanup failed: ${response.statusText}`);
      }
    }, projectName);
  }
}
