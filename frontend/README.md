# 抽奖前端项目

基于 Vue 3 + TypeScript + Element Plus 的高并发抽奖平台前端。

## 功能特性

- 🎉 用户登录系统
- 🎁 奖品展示
- 🎯 抽奖功能（带动画效果）
- 📊 用户账户信息展示
- ✅ 每日签到功能
- 📱 响应式设计

## 技术栈

- **框架**: Vue 3 + TypeScript
- **UI组件库**: Element Plus
- **HTTP客户端**: Axios
- **构建工具**: Vite

## 快速开始

### 1. 安装依赖
```bash
npm install
```

### 2. 启动开发服务器
```bash
npm run dev
```

访问 http://localhost:5173 查看应用

### 3. 构建生产版本
```bash
npm run build
```

## API配置

在 `src/services/api.ts` 中配置后端API地址：

```typescript
const API_BASE_URL = 'http://localhost:8000'  // 修改为你的后端地址
```

## 使用说明

1. **用户登录**: 输入用户ID进行登录
2. **查看奖品**: 登录后自动加载奖品列表
3. **抽奖**: 点击"开始抽奖"按钮，有动画效果
4. **签到**: 每日可签到一次，增加抽奖次数
5. **查看账户**: 显示剩余抽奖次数信息

## 项目结构

```
src/
├── components/         # Vue组件
│   └── RafflePage.vue  # 主抽奖页面
├── services/          # API服务
│   └── api.ts        # 接口调用
├── App.vue           # 根组件
├── main.ts          # 入口文件
└── style.css        # 全局样式
```

## 注意事项

- 确保后端服务已启动并运行在配置的端口上
- 默认使用活动ID: 1001，策略ID: 1001
- 抽奖次数受用户账户限制

## 开发规范

遵循项目原有的开发规范，使用 TypeScript 进行类型安全的开发。