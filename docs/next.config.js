const withNextra = require('nextra')({
  theme: 'nextra-theme-docs',
  themeConfig: './theme.config.tsx',
  defaultShowCopyCode: true,
  themeSwitch: {
    useOptions() {
      return {
        light: 'Light',
        dark: 'Dark',
        system: 'System'
      }
    }
  }
})

module.exports = withNextra({
  webpack: (config, { dev, isServer }) => {
    if (!dev) {
      config.cache = false;
    }
    return config;
  },
  async redirects() {
    // TODO: Configure Redirects
    return [
      {
        source: '/test',
        destination: '/testing',
        permanent: true,
      },
    ]
  },
})
