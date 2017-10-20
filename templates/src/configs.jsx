import DashboardView from "./DashboardView";

export const VIEWS_CONFIG = {
  CONFIG: [
    // Insert views and their path mappings
    {component: DashboardView, path: '/'}
  ],
};

export const NAVBAR_CONFIG = {
  // The different tabs and the link to where they are routed to.
  categories: [
    {title: 'Dashboard', link: '/'}
  ],
  titleSrc: {
    path: 'images/brand.svg',
    alt: 'Radanalytics-Image-Recognition-Logo'
  }
};

export const MODALS = {
  FEEDBACK_MODAL : "001",
  CONFIG_HELP_MODAL: "002"
};
