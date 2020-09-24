export default class UiMCAManagerPlugin {
  registerRoutes(routes) {
    routes.push({
      name: 'hexiMCAManagerConfig',
      parent: 'hexiLayoutPage',
      path: '/core/mcaManager/config',
      component: require('./Config/index.vue'),
      meta: {
        title: 'MCA Manager',
      },
    });
    routes.push({
      name: 'hexiMCAManagerConfigActivatedPlugin',
      parent: 'hexiMCAManagerConfig',
      path: '/core/mcaManager/config/activatedPlugin',
      component: require('./Config/activatedPlugin.vue'),
      meta: {
        title: 'Select Algorithm',
      },
    });
    routes.push({
      name: 'hexiMCAManagerConfigLogs',
      parent: 'hexiMCAManagerConfig',
      path: '/core/mcaManager/config/logs',
      component: require('./Config/logs.vue'),
      meta: {
        title: 'MCA Logs',
      },
    });
  }

  registerSidebarMenus(menus) {
    menus.push({
      name: 'hexiMCAManagerConfig',
      index: '/core/mcaManager/config',
      title: 'MCA Manager',
    });
  }
}
