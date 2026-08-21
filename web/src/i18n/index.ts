import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import en from './locales/en.json';
import th from './locales/th.json';

export const LANGUAGES = ['en', 'th'] as const;
export type Language = (typeof LANGUAGES)[number];

const STORAGE_KEY = 'ltd.language';

function initialLanguage(): Language {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'en' || stored === 'th') return stored;
  return navigator.language.startsWith('th') ? 'th' : 'en';
}

void i18n.use(initReactI18next).init({
  resources: { en: { translation: en }, th: { translation: th } },
  lng: initialLanguage(),
  fallbackLng: 'en',
  interpolation: { escapeValue: false },
});

export function setLanguage(lang: Language): void {
  localStorage.setItem(STORAGE_KEY, lang);
  void i18n.changeLanguage(lang);
  document.documentElement.lang = lang;
}

document.documentElement.lang = i18n.language;

export default i18n;
