export default class UiOutputManagerPlugin {
  registerRoutes(routes) {
    routes.push({
      name: 'hexiOutputManagerConfig',
      parent: 'hexiLayoutPage',
      path: '/core/outputManager/config',
      component: require('./Config/index.vue'),
      meta: {
        title: 'Output Manager',
      },
    });
    routes.push({
      name: 'hexiOutputManagerConfigActivatedPlugin',
      parent: 'hexiOutputManagerConfig',
      path: '/core/outputManager/config/activatedPlugin',
      component: require('./Config/activatedPlugin.vue'),
      meta: {
        title: '选择目标体感平台',
      },
    });
  }

  registerSidebarMenus(menus) {
    menus.push({
      name: 'hexiOutputManagerConfig',
      index: '/core/outputManager/config',
      title: 'Output Manager',
    });
  }
}
