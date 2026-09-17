import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Urutau',
  tagline: 'CDC engine — replicate MySQL, Postgres, and Kafka into Iceberg, ClickHouse, and Couchbase',
  favicon: 'img/icon.svg',

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
    image: 'img/docusaurus-social-card.jpg',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Urutau',
      logo: {
        src: 'img/icon.svg',
        srcDark: 'img/icon-dark.svg',
        alt: 'Urutau',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://pkg.go.dev/github.com/maltzsama/urutau',
          label: 'Go API',
          position: 'right',
        },
        {
          href: 'https://github.com/maltzsama/urutau',
          label: 'GitHub',
          position: 'right',
          className: 'header-github-link',
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
            {label: 'Delivery guarantees', to: '/docs/reference/guarantees'},
            {label: 'Go API reference', href: 'https://pkg.go.dev/github.com/maltzsama/urutau'},
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
