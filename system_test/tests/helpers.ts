import { Page, expect } from '@playwright/test';
import { spawn, ChildProcess } from 'child_process';
import { promises as fs } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

export class FeatherboxTestHelper {
  private server?: ChildProcess;
  private projectPath?: string;
  private projectName?: string;

  async setupProject(projectName: string = 'test_project'): Promise<string> {
    this.projectName = projectName;
    
    const tempDir = await fs.mkdtemp(join(tmpdir(), 'featherbox-e2e-'));
    this.projectPath = join(tempDir, projectName);
    
    console.log(`Creating test project: ${this.projectPath}`);
    
    // Clean up any existing project directory in the current working directory
    const currentWorkingDir = process.cwd();
    const existingProject = join(currentWorkingDir, projectName);
    try {
      await fs.rm(existingProject, { recursive: true, force: true });
      console.log(`Cleaned up existing project: ${existingProject}`);
    } catch (error) {
      // Ignore if directory doesn't exist
    }
    
    // Get the featherbox project root (one level up from e2e directory)
    const featherboxRoot = join(__dirname, '..');
    
    const createProcess = spawn('bash', ['-c', `cd "${tempDir}" && "${featherboxRoot}/target/debug/featherbox" new ${projectName}`], {
      cwd: featherboxRoot,
      stdio: ['ignore', 'pipe', 'pipe']
    });

    return new Promise((resolve, reject) => {
      let output = '';
      let error = '';
      
      createProcess.stdout?.on('data', (data) => {
        output += data.toString();
      });
      
      createProcess.stderr?.on('data', (data) => {
        error += data.toString();
      });

      createProcess.on('close', async (code) => {
        if (code === 0) {
          console.log('Project created successfully');
          
          // Create test CSV file
          const csvContent = `id,name,email,created_at
1,John Doe,john@example.com,2024-01-01
2,Jane Smith,jane@example.com,2024-01-02
3,Bob Johnson,bob@example.com,2024-01-03`;
          
          const csvPath = join(this.projectPath!, 'users.csv');
          await fs.writeFile(csvPath, csvContent);
          console.log('Created test CSV file');
          
          resolve(this.projectPath!);
        } else {
          console.error('Failed to create project:', error);
          reject(new Error(`Failed to create project: ${error}`));
        }
      });
    });
  }

  async startServer(projectPath?: string): Promise<void> {
    const path = projectPath || this.projectPath;
    if (!path) {
      throw new Error('No project path specified');
    }

    console.log(`Starting server for project: ${path}`);
    
    // Get the featherbox project root
    const featherboxRoot = join(__dirname, '..');
    
    this.server = spawn('bash', ['-c', `cd "${path}" && "${featherboxRoot}/target/debug/featherbox" start .`], {
      cwd: featherboxRoot,
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let serverReady = false;
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (!serverReady) {
          reject(new Error('Server startup timeout'));
        }
      }, 30000);

      this.server!.stdout?.on('data', (data) => {
        const output = data.toString();
        console.log('Server output:', output);
        
        if (output.includes('API server started') || output.includes('Featherbox is running') || output.includes('Starting UI development server')) {
          serverReady = true;
          clearTimeout(timeout);
          setTimeout(resolve, 5000); // UIサーバー起動により長い時間が必要
        }
      });

      this.server!.stderr?.on('data', (data) => {
        const error = data.toString();
        console.error('Server error:', error);
        
        if (error.includes('listening on') || error.includes('Server started')) {
          serverReady = true;
          clearTimeout(timeout);
          setTimeout(resolve, 2000);
        }
      });

      this.server!.on('close', (code) => {
        if (code !== 0 && !serverReady) {
          clearTimeout(timeout);
          reject(new Error(`Server exited with code ${code}`));
        }
      });
    });
  }

  async stopServer(): Promise<void> {
    if (this.server) {
      console.log('Stopping server...');
      
      return new Promise((resolve) => {
        let resolved = false;
        
        const handleResolve = () => {
          if (!resolved) {
            resolved = true;
            console.log('Server stopped');
            this.server = null;
            resolve();
          }
        };
        
        this.server!.on('close', handleResolve);
        this.server!.on('exit', handleResolve);
        
        // Force kill after timeout
        setTimeout(() => {
          if (this.server && !resolved) {
            this.server.kill('SIGKILL');
            handleResolve();
          }
        }, 3000);
        
        // Start with graceful termination
        this.server.kill('SIGTERM');
      });
    }
  }

  async cleanup(): Promise<void> {
    try {
      await this.stopServer();
    } catch (error) {
      console.warn('Server stop error:', error);
    }
    
    if (this.projectPath) {
      try {
        await fs.rm(this.projectPath, { recursive: true, force: true });
        console.log(`Cleaned up project: ${this.projectPath}`);
      } catch (error) {
        console.warn(`Failed to cleanup project: ${error}`);
      }
    }
  }

  async waitForPageLoad(page: Page): Promise<void> {
    await page.waitForLoadState('domcontentloaded');
    await page.waitForTimeout(1000);
  }

  async navigateToSection(page: Page, section: 'connections' | 'adapters' | 'models' | 'pipeline' | 'query' | 'dashboards'): Promise<void> {
    const sectionTitles = {
      connections: 'Connections',
      adapters: 'Adapters', 
      models: 'Models',
      pipeline: 'Pipeline',
      query: 'Query',
      dashboards: 'Dashboards'
    };

    await page.getByRole('button', { name: sectionTitles[section] }).click();
    await page.waitForTimeout(500);
  }

  async createConnection(page: Page, connectionData: {
    name: string;
    type: string;
    host?: string;
    database?: string;
    username?: string;
    password?: string;
  }): Promise<void> {
    await this.navigateToSection(page, 'connections');
    
    await page.getByRole('button', { name: /new connection/i }).click();
    
    await page.fill('[data-testid="connection-name"]', connectionData.name);
    await page.selectOption('[data-testid="connection-type"]', connectionData.type);
    
    if (connectionData.host) {
      await page.fill('[data-testid="connection-host"]', connectionData.host);
    }
    if (connectionData.database) {
      await page.fill('[data-testid="connection-database"]', connectionData.database);
    }
    if (connectionData.username) {
      await page.fill('[data-testid="connection-username"]', connectionData.username);
    }
    if (connectionData.password) {
      await page.fill('[data-testid="connection-password"]', connectionData.password);
    }
    
    await page.getByRole('button', { name: /save|create/i }).click();
    await this.waitForPageLoad(page);
  }

  async verifyConnectionExists(page: Page, connectionName: string): Promise<void> {
    await this.navigateToSection(page, 'connections');
    await expect(page.locator(`text=${connectionName}`)).toBeVisible();
  }

  async deleteConnection(page: Page, connectionName: string): Promise<void> {
    await this.navigateToSection(page, 'connections');
    
    const connectionRow = page.locator(`[data-testid="connection-${connectionName}"]`);
    await connectionRow.getByRole('button', { name: /delete/i }).click();
    
    await page.getByRole('button', { name: /confirm|yes|delete/i }).click();
    await this.waitForPageLoad(page);
  }

  async createAdapter(page: Page, adapterData: {
    name: string;
    type: 'csv' | 'json' | 'parquet';
    source: string;
  }): Promise<void> {
    await this.navigateToSection(page, 'adapters');
    
    await page.getByRole('button', { name: /new adapter/i }).click();
    
    await page.fill('[data-testid="adapter-name"]', adapterData.name);
    await page.selectOption('[data-testid="adapter-type"]', adapterData.type);
    await page.fill('[data-testid="adapter-source"]', adapterData.source);
    
    await page.getByRole('button', { name: /save|create/i }).click();
    await this.waitForPageLoad(page);
  }

  async createModel(page: Page, modelData: {
    name: string;
    sql: string;
  }): Promise<void> {
    await this.navigateToSection(page, 'models');
    
    await page.getByRole('button', { name: /new model/i }).click();
    
    await page.fill('[data-testid="model-name"]', modelData.name);
    await page.fill('[data-testid="model-sql"]', modelData.sql);
    
    await page.getByRole('button', { name: /save|create/i }).click();
    await this.waitForPageLoad(page);
  }

  async executePipeline(page: Page): Promise<void> {
    await this.navigateToSection(page, 'pipeline');
    
    await page.getByRole('button', { name: /migrate/i }).click();
    await this.waitForPageLoad(page);
    
    await page.getByRole('button', { name: /run/i }).click();
    await this.waitForPageLoad(page);
  }

  async executeQuery(page: Page, sql: string): Promise<void> {
    await this.navigateToSection(page, 'query');
    
    await page.fill('[data-testid="query-sql"]', sql);
    await page.getByRole('button', { name: /execute/i }).click();
    await this.waitForPageLoad(page);
  }

  async verifyQueryResults(page: Page, expectedText: string): Promise<void> {
    await expect(page.locator('[data-testid="query-results"]')).toContainText(expectedText);
  }
}

export const testData = {
  connection: {
    name: 'test_connection',
    type: 'sqlite',
    database: 'test.db'
  },
  adapter: {
    name: 'test_adapter', 
    type: 'csv' as const,
    source: 'test_data.csv'
  },
  model: {
    name: 'test_model',
    sql: 'SELECT * FROM test_adapter'
  }
};