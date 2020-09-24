<template>
  <ui-section-container key="page-mca-manager-config">
    <ui-section title="MCA Manager" width="300px">
      <ui-section-content extend>
        <ui-menu-list>
          <ui-menu-list-item :href="{ name: 'hexiMCAManagerConfigActivatedPlugin' }">
            <i class="fa fa-code" aria-hidden="true"></i> Algorithm
          </ui-menu-list-item>
          <ui-menu-list-item :href="{ name: 'hexiMCAManagerConfigLogs' }">
            <i class="fa fa-bug" aria-hidden="true"></i> MCA Logs
          </ui-menu-list-item>
        </ui-menu-list>
      </ui-section-content>
    </ui-section>
    <transition name="transition-page" mode="out-in">
      <router-view></router-view>
    </transition>
  </ui-section-container>
</template>

<script>
import API from '@core/utils/api';

export default {
  name: 'page-mca-manager-config',
}
</script>
