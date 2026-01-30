interface ImportMetaEnv {
  readonly VITE_RECAPTCHA_SITE_KEY?: string;
  readonly VITE_RECAPTCHA_ENABLED?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
