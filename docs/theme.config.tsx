import { useRouter } from 'next/router'
import React from 'react'
import Logo from "./components/logo";
import { DocsThemeConfig } from 'nextra-theme-docs'

const config: DocsThemeConfig = {
  logo: () => {
    return (
      <>
        <Logo height={27} />
        <span
          className="mx-2 font-extrabold hidden md:inline select-none text-xl"
          title={`Indexify Docs`}
          style={{ marginLeft: '5px'}}
        >
          Docs
        </span>
      </>
    );
  },
  project: {
    link: 'https://github.com/adithyaakrishna/indexify-docs',
  },
  chat: {
    link: 'https://dub.sh/tensorlake-discord',
  },
  docsRepositoryBase: 'https://github.com/adithyaakrishna/indexify-docs',
  useNextSeoProps() {
    const { asPath } = useRouter()
    if (asPath !== '/') {
      return {
        titleTemplate: '%s'
      }
    }
  },
  footer: {
    text: (
      <span>
        {' '}Indexify by{' '}
        <a href="https://tensorlake.ai" target="_blank">
          Tensorlake
        </a>
        {' '}©{' '}{new Date().getFullYear()}
      </span>
    )
  },
}

export default config
