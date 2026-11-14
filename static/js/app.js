const { createApp } = Vue;

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      busy: false,
      tab: "board",
      overview: { partitions: [], events: [], policy: null, policy_locked_today: false, today: "" },
      partitionDetail: null,
      openWrite: null,
      form: { partition_key: "", method: "direct", writer: "", row_count: null },
      policyForm: { strategy: "fixed_days", window_days: 3 },
      toast: "",
      toastKind: "ok",
    };
  },
  async mounted() {
    const me = await fetch("/api/me");
    if (!me.ok) {
      location.href = "/login";
      return;
    }
    this.me = (await me.json()).user;
    await this.refresh();
  },
  methods: {
    tagClass(status) {
      if (status === "已落库") return "ok";
      if (status === "已抽取待加载") return "warn";
      return "";
    },
    eventTagClass(e) {
      if (e.status === "rejected") return "danger";
      if (e.event === "重写成功" || e.event === "落库成功") return "ok";
      if (e.event === "写入中") return "warn";
      if (e.event === "清空") return "out";
      return "";
    },
    eventRowClass(e) {
      return { rejectrow: e.status === "rejected" };
    },
    flash(msg, kind = "ok") {
      this.toast = msg;
      this.toastKind = kind;
      window.setTimeout(() => (this.toast = ""), 4200);
    },
    async handle(res) {
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        this.flash(data.error || "请求被拒绝", "err");
        return null;
      }
      return data;
    },
    async refresh() {
      const res = await fetch("/api/pipelines");
      if (!res.ok) return;
      this.items = (await res.json()).items || [];
    },
    async refreshAll() {
      if (!this.current) return;
      await this.refresh();
      const [cur, ov] = await Promise.all([
        fetch(`/api/pipelines/${this.current.id}`),
        fetch(`/api/pipelines/${this.current.id}/backfill/overview`),
      ]);
      if (cur.ok) this.current = await cur.json();
      if (ov.ok) {
        this.overview = await ov.json();
        if (this.overview.policy) {
          this.policyForm.strategy = this.overview.policy.strategy;
          this.policyForm.window_days = this.overview.policy.window_days ?? 3;
        }
      }
    },
    async open(id) {
      this.openWrite = null;
      this.partitionDetail = null;
      const res = await fetch(`/api/pipelines/${id}`);
      if (!res.ok) return;
      this.current = await res.json();
      await this.refreshAll();
    },
    async extract() {
      this.busy = true;
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/extract`, { method: "POST" });
        const data = await this.handle(res);
        if (data) this.flash("抽取完成，可继续落库");
        await this.refreshAll();
      } finally {
        this.busy = false;
      }
    },
    async load() {
      this.busy = true;
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/load`, { method: "POST" });
        const data = await this.handle(res);
        if (data) this.flash(data.status === "已落库" ? "基线落库完成" : "当前状态不能落库，请先抽取");
        await this.refreshAll();
      } finally {
        this.busy = false;
      }
    },
    _body() {
      const b = {
        partition_key: this.form.partition_key,
        method: this.form.method,
        writer: this.form.writer || undefined,
      };
      if (this.form.row_count) b.row_count = this.form.row_count;
      return b;
    },
    async oneShot() {
      this.busy = true;
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/partitions/write`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(this._body()),
        });
        const data = await this.handle(res);
        if (data) {
          this.flash(`${data.event}：分区 ${data.partition.partition_key} 已落 ${data.partition.row_count} 行`);
          this.form.partition_key = "";
        }
        await this.refreshAll();
      } finally {
        this.busy = false;
      }
    },
    async begin() {
      this.busy = true;
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/partitions/begin`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(this._body()),
        });
        const data = await this.handle(res);
        if (data) {
          this.openWrite = data;
          this.flash(`写入单 #${data.write_id} 已开启（${data.kind === "backfill" ? "回溯重落" : "首次落库"}），待提交`);
        }
        await this.refreshAll();
      } finally {
        this.busy = false;
      }
    },
    async commit() {
      if (!this.openWrite) return;
      this.busy = true;
      try {
        const body = this.form.row_count ? { row_count: this.form.row_count } : {};
        const res = await fetch(`/api/writes/${this.openWrite.write_id}/commit`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await this.handle(res);
        if (data) {
          this.flash(`${data.event}：分区 ${data.partition.partition_key} 现为 ${data.partition.row_count} 行`);
          this.openWrite = null;
          this.form.partition_key = "";
        }
        await this.refreshAll();
      } finally {
        this.busy = false;
      }
    },
    async rollback() {
      if (!this.openWrite) return;
      this.busy = true;
      try {
        const res = await fetch(`/api/writes/${this.openWrite.write_id}/rollback`, { method: "POST" });
        const data = await this.handle(res);
        if (data) {
          this.flash("写入单已回滚，未落任何数据");
          this.openWrite = null;
        }
        await this.refreshAll();
      } finally {
        this.busy = false;
      }
    },
    async openPartition(pid) {
      this.tab = "events";
      const res = await fetch(`/api/pipelines/${this.current.id}/partitions/${pid}`);
      if (res.ok) this.partitionDetail = await res.json();
    },
    async savePolicy() {
      this.busy = true;
      try {
        const body = { strategy: this.policyForm.strategy };
        if (body.strategy === "fixed_days") body.window_days = this.policyForm.window_days;
        const res = await fetch(`/api/pipelines/${this.current.id}/backfill/policy`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await this.handle(res);
        if (data) this.flash(`回溯策略已写入并本日锁死：${data.policy.policy_name}`);
        await this.refreshAll();
      } finally {
        this.busy = false;
      }
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
