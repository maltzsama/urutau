import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useColorMode} from '@docusaurus/theme-common';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  const {colorMode} = useColorMode();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <img
          src={colorMode === 'dark' ? '/urutau/img/icon.svg' : '/urutau/img/icon-dark.svg'}
          alt="Urutau"
          className={styles.heroLogo}
          width={120}
          height={120}
        />
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className={styles.etymology}>
          Tupi–Guaraní for the potoo — a nightjar that stands motionless through
          the night, watching. A fitting name for a process that spends its life
          quietly watching a binlog.
        </p>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className={clsx('button button--lg', styles.outlineButton)}
            to="/docs/quickstart">
            Get Started
          </Link>
          <iframe
            src="https://ghbtns.com/github-btn.html?user=maltzsama&repo=urutau&type=star&count=true&size=large"
            width="160"
            height="30"
            title="GitHub Stars"
            className={styles.githubButton}
          />
        </div>
      </div>
    </header>
  );
}

const features = [
  {
    title: 'No New Broker',
    description: 'Reads from MySQL, Postgres, or an existing Kafka/Redpanda topic and writes straight to the destination — no new broker or relay to stand up.',
  },
  {
    title: 'Upsert Reflects State',
    description: 'UPDATE/DELETE changes the row in the destination. No appended duplicates, no tombstone sprawl.',
  },
  {
    title: 'No JVM Sidecar',
    description: 'Writes natively from the same Go binary that reads the log — standalone in one process, or coordinator+worker distributed via the operator. No JAR, no gRPC hop to a second runtime.',
  },
];

function Feature({title, description}: {title: string; description: string}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md padding-vert--lg">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {features.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description={siteConfig.tagline}>
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
