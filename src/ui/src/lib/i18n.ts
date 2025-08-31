import {
  register,
  init,
  getLocaleFromNavigator,
  locale,
  isLoading,
} from 'svelte-i18n';

register('en', () => import('./locales/en.json'));
register('ja', () => import('./locales/ja.json'));

let initialLocale = 'ja';

if (typeof window !== 'undefined') {
  const savedLocale = localStorage.getItem('locale');
  initialLocale = savedLocale || getLocaleFromNavigator() || 'ja';
}

init({
  fallbackLocale: 'ja',
  initialLocale,
});

if (typeof window !== 'undefined') {
  locale.subscribe((value: string | null | undefined) => {
    if (value) {
      localStorage.setItem('locale', value);
    }
  });
}

export { locale, isLoading };
export { _ as t } from 'svelte-i18n';
