<template>
  <ui-section-container key="page-input-fsx-plugin-config-data-options" v-loading.body="loading">
    <ui-section title="Data Source Config" width="300px">
      <ui-section-content>
        <el-form ref="form" :model="data" label-position="top">
          <el-form-item key="tcp_host" label="TCP Host">
            <el-input v-model="data.tcp_host"></el-input>
          </el-form-item>
          <el-form-item key="tcp_port" label="TCP Port">
            <el-input v-model="data.tcp_port"></el-input>
          </el-form-item>
          <el-form-item key="udp_port" label="UDP Port">
            <el-input v-model="data.udp_port"></el-input>
          </el-form-item>
          <el-form-item>
            <el-button @click="submit()">Save</el-button>
            <el-button @click="cancel()">Cancel</el-button>
          </el-form-item>
        </el-form>
      </ui-section-content>
    </ui-section>
  </ui-section-container>
</template>

<script>
import API from '@module/api';

export default {
  name: 'page-input-fsx-plugin-config-data-options',
  data() {
    return {
      data: {},
      loading: false,
    };
  },
  created() {
    this.initData();
  },
  methods: {
    async initData() {
      this.loading = true;
      try {
        this.data = (await API.config.get()).data;
      } finally {
        this.loading = false;
      }
    },
    async submit() {
      await API.config.set(this.data);
      this.$notify({
        title: '成功',
        message: '配置更新成功',
        type: 'success'
      });
    },
    cancel() {
      this.$router.go(-1);
    },
  },
}
</script>
