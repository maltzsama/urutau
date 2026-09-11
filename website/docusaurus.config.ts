import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Urutau',
  tagline: 'CDC engine — replicate MySQL, Postgres, and Kafka into Iceberg, ClickHouse, and Couchbase',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://maltzsama.github.io',
  baseUrl: '/urutau/',

  organizationName: 'maltzsama',
  projectName: 'urutau',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/maltzsama/urutau/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  markdown: {
    mermaid: true,
  },

  themes: ['@docusaurus/theme-mermaid'],

  themeConfig: {
    image: 'img/social-card.png',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Urutau',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/maltzsama/urutau',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Quickstart', to: '/docs/quickstart'},
            {label: 'Architecture', to: '/docs/architecture/overview'},
            {label: 'Semantics', to: '/docs/reference/semantics'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub', href: 'https://github.com/maltzsama/urutau'},
            {label: 'Issues', href: 'https://github.com/maltzsama/urutau/issues'},
          ],
        },
      ],
      copyright: `Copyright \u00a9 ${new Date().getFullYear()} Urutau contributors. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['go', 'yaml', 'sql'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
