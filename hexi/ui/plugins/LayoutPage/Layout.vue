<template>
  <div class="vcContainer">
    <div class="layoutLeft">
      <el-menu
        class="leftMenu"
        theme="dark"
        :default-active="$route.fullPath"
        :default-openeds="['problem_manage', 'answer_manage', 'user']"
        router
      >
        <component v-for="menu in sidebarMenus" v-bind:key="menu.index" :is="getMenuType(menu)" :index="menu.index">
          <span v-if="!menu.hasChildren">{{ menu.title }}</span>
          <template slot="title" v-if="menu.hasChildren">{{ menu.title }}</template>
          <el-menu-item key="submenu.index" v-for="submenu in menu.children" v-bind:key="submenu.index" :index="submenu.index">{{ submenu.title }}</el-menu-item>
        </component>

      </el-menu>
    </div>
    <div class="layoutMain">
      <transition name="transition-page" mode="out-in">
        <router-view></router-view>
      </transition>
    </div>
  </div>
</template>

<script>
import { mapState } from 'vuex';

export default {
  name: 'layout',
  computed: {
    ...mapState('layout', [
      'sidebarMenus',
    ]),
  },
  methods: {
    getMenuType(menuItem) {
      if (menuItem.hasChildren) {
        return 'el-submenu';
      } else {
        return 'el-menu-item';
      }
    },
    handleSelect(index) {
      this.$router.push(index);
    },
  },
}
</script>

<style scoped lang="stylus">
.vcContainer
  width: 100%
  height: 100%
  overflow: hidden
  position: relative

.layoutLeft, .layoutMain
  position: absolute

.layoutLeft
  left: 0
  top: 0
  width: 240px
  height: 100%

.layoutMain
  left: 240px
  top: 0
  right: 0
  height: 100%
  background: oc-gray-1
  overflow-x: auto
  white-space: nowrap
  font-size: 0

.leftMenu
  height: 100%
</style>
