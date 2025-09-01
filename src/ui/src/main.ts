import { mount } from 'svelte';
import './app.css';
import './lib/i18n';
import App from './App.svelte';
import { isLoading } from './lib/i18n';

let app: any;

const initApp = () => {
  if (app) return;

  app = mount(App, {
    target: document.getElementById('app')!,
  });
};

isLoading.subscribe((loading) => {
  if (!loading) {
    initApp();
  }
});

export default app;
