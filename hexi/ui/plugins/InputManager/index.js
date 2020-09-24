export default class UiInputManagerPlugin {
  registerRoutes(routes) {
    routes.push({
      name: 'hexiInputManagerConfig',
      parent: 'hexiLayoutPage',
      path: '/core/inputManager/config',
      component: require('./Config/index.vue'),
      meta: {
        title: 'Config',
      },
    });
    routes.push({
      name: 'hexiInputManagerConfigActivatedPlugin',
      parent: 'hexiInputManagerConfig',
      path: '/core/inputManager/config/activatedPlugin',
      component: require('./Config/activatedPlugin.vue'),
      meta: {
        title: 'Activated Plugins',
      },
    });
    routes.push({
      name: 'hexiInputManagerConfigLogs',
      parent: 'hexiInputManagerConfig',
      path: '/core/inputManager/config/logs',
      component: require('./Config/logs.vue'),
      meta: {
        title: 'Logs',
      },
    });
  }

  registerSidebarMenus(menus) {
    menus.push({
      name: 'hexiInputManagerConfig',
      index: '/core/inputManager/config',
      title: 'Input Manager',
    });
  }
}
