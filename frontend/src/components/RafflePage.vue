<template>
  <div class="raffle-container">
    <el-card class="raffle-card">
      <template #header>
        <div class="card-header">
          <span>🎉 幸运抽奖</span>
          <el-tag type="info">高并发抽奖平台</el-tag>
        </div>
      </template>

      <!-- 用户信息区域 -->
      <div class="user-section" v-if="userInfo.userId">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="用户ID">{{ userInfo.userId }}</el-descriptions-item>
          <el-descriptions-item label="今日剩余次数">
            <el-tag type="success">{{ userAccount.day_count_surplus }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="本月剩余次数">
            <el-tag type="warning">{{ userAccount.month_count_surplus }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="总剩余次数">
            <el-tag type="danger">{{ userAccount.total_count_surplus }}</el-tag>
          </el-descriptions-item>
        </el-descriptions>
      </div>

      <!-- 登录区域 -->
      <div class="login-section" v-else>
        <el-form :inline="true" @submit.prevent="login">
          <el-form-item label="用户ID">
            <el-input 
              v-model="loginForm.userId" 
              placeholder="请输入用户ID"
              style="width: 200px"
            />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="login">登录</el-button>
          </el-form-item>
        </el-form>
      </div>

      <!-- 抽奖区域 -->
      <div class="raffle-section" v-if="userInfo.userId">
        <!-- 奖品展示 -->
        <div class="awards-display" v-if="awards.length > 0">
          <h3>🎁 奖品列表</h3>
          <div class="awards-grid">
            <div 
              v-for="(award, index) in awards" 
              :key="award.award_id"
              class="award-item"
              :class="{ 'highlight': currentHighlight === index }"
            >
              <div class="award-icon">🎁</div>
              <div class="award-title">{{ award.award_title }}</div>
              <div class="award-subtitle">{{ award.award_subtitle }}</div>
            </div>
          </div>
        </div>

        <!-- 装配与抽奖按钮 -->
        <div class="raffle-controls">
          <el-button
            type="warning"
            size="large"
            :loading="isArmoring"
            @click="handleArmory"
            class="armory-button"
          >
            {{ isArmoring ? '装配中...' : '执行装配' }}
          </el-button>

          <el-button 
            type="primary" 
            size="large"
            :loading="isDrawing"
            :disabled="!canDraw || isDrawing"
            @click="startRaffle"
            class="raffle-button"
          >
            {{ isDrawing ? '抽奖中...' : '开始抽奖' }}
          </el-button>
          
          <el-button 
            type="success" 
            size="large"
            :loading="isSigning"
            :disabled="hasSigned || isSigning"
            @click="calendarSign"
            class="sign-button"
          >
            {{ hasSigned ? '已签到' : '每日签到' }}
          </el-button>
        </div>

        <!-- 抽奖结果 -->
        <div class="result-section" v-if="drawResult">
          <el-alert
            :title="`🎉 恭喜您获得：${drawResult.award_title}`"
            type="success"
            :closable="false"
            show-icon
          />
        </div>
      </div>

      <!-- 加载状态 -->
      <div class="loading-section" v-if="loading">
        <el-skeleton :rows="3" animated />
      </div>

      <!-- 错误提示 -->
      <div class="error-section" v-if="error">
        <el-alert
          :title="error"
          type="error"
          :closable="true"
          @close="error = ''"
        />
      </div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { activityAPI, strategyAPI } from '../services/api'
import type { RaffleAward, DrawReply, UserActivityAccount } from '../services/api'

// 活动配置
const ACTIVITY_ID = 100302 // 默认活动ID
const STRATEGY_ID = 100001 // 默认策略ID

// 状态管理
const loading = ref(false)
const error = ref('')
const isArmoring = ref(false)
const isDrawing = ref(false)
const isSigning = ref(false)

// 用户信息
const userInfo = ref({
  userId: ''
})

// 登录表单
const loginForm = ref({
  userId: ''
})

// 数据状态
const awards = ref<RaffleAward[]>([])
const userAccount = ref<UserActivityAccount>({
  activity_id: 0,
  total_count: 0,
  total_count_surplus: 0,
  day_count: 0,
  day_count_surplus: 0,
  month_count: 0,
  month_count_surplus: 0
})
const hasSigned = ref(false)
const drawResult = ref<DrawReply | null>(null)
const currentHighlight = ref<number>(-1)

// 计算属性
const canDraw = computed(() => {
  return userAccount.value.day_count_surplus > 0
})

// 登录
const login = async () => {
  if (!loginForm.value.userId.trim()) {
    ElMessage.warning('请输入用户ID')
    return
  }

  const userId = loginForm.value.userId.trim()
  loading.value = true
  error.value = ''

  try {
    await activityAPI.loadUserActivityAccount(userId, ACTIVITY_ID)
    userInfo.value.userId = userId
    await loadUserData()
  } catch (err: any) {
    error.value = err.message || '登录初始化失败'
    ElMessage.error(error.value)
  } finally {
    loading.value = false
  }
}

// 加载用户数据
const loadUserData = async () => {
  if (!userInfo.value.userId) return
  
  loading.value = true
  error.value = ''
  
  try {
    // 并行加载所有数据
    const [awardsData, accountData, signStatus] = await Promise.all([
      strategyAPI.queryRaffleAwardList(STRATEGY_ID, userInfo.value.userId),
      activityAPI.queryUserActivityAccount(userInfo.value.userId, ACTIVITY_ID),
      activityAPI.isCalendarSignRebate(userInfo.value.userId)
    ])
    
    awards.value = awardsData
    userAccount.value = accountData
    hasSigned.value = signStatus
    
    ElMessage.success('数据加载成功')
  } catch (err: any) {
    error.value = err.message || '加载数据失败'
    ElMessage.error(error.value)
  } finally {
    loading.value = false
  }
}

// 手动装配活动和策略
const handleArmory = async () => {
  isArmoring.value = true
  error.value = ''

  try {
    await activityAPI.armoryActivity(ACTIVITY_ID)
    await strategyAPI.armoryStrategy(STRATEGY_ID)
    ElMessage.success('装配成功')
  } catch (err: any) {
    error.value = err.message || '装配失败'
    ElMessage.error(error.value)
  } finally {
    isArmoring.value = false
  }
}

// 开始抽奖
const startRaffle = async () => {
  if (!userInfo.value.userId || !canDraw.value) return
  
  isDrawing.value = true
  error.value = ''
  drawResult.value = null
  
  try {
    // 抽奖动画效果
    await animateRaffle()
    
    // 执行抽奖
    const result = await activityAPI.draw(userInfo.value.userId, ACTIVITY_ID)
    drawResult.value = result
    
    // 更新用户账户信息
    await loadUserData()
    
    ElMessage.success(`恭喜获得：${result.award_title}`)
  } catch (err: any) {
    error.value = err.message || '抽奖失败'
    ElMessage.error(error.value)
  } finally {
    isDrawing.value = false
    currentHighlight.value = -1
  }
}

// 抽奖动画
const animateRaffle = async () => {
  const duration = 3000 // 3秒动画
  const interval = 100 // 每100ms切换一次
  const cycles = duration / interval
  
  for (let i = 0; i < cycles; i++) {
    currentHighlight.value = Math.floor(Math.random() * awards.value.length)
    await new Promise(resolve => setTimeout(resolve, interval))
    
    // 逐渐减速
    if (i > cycles * 0.7) {
      await new Promise(resolve => setTimeout(resolve, interval * 2))
    }
  }
}

// 每日签到
const calendarSign = async () => {
  if (!userInfo.value.userId || hasSigned.value) return
  
  isSigning.value = true
  error.value = ''
  
  try {
    const success = await activityAPI.calendarSignRebate(userInfo.value.userId)
    if (success) {
      hasSigned.value = true
      await loadUserData()
      ElMessage.success('签到成功')
    }
  } catch (err: any) {
    error.value = err.message || '签到失败'
    ElMessage.error(error.value)
  } finally {
    isSigning.value = false
  }
}

</script>

<style scoped>
.raffle-container {
  min-height: 100vh;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  padding: 20px;
  display: flex;
  justify-content: center;
  align-items: center;
}

.raffle-card {
  width: 100%;
  max-width: 800px;
  border-radius: 15px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 24px;
  font-weight: bold;
  color: #303133;
}

.user-section {
  margin-bottom: 30px;
}

.login-section {
  text-align: center;
  margin-bottom: 30px;
  padding: 20px;
  background: #f5f7fa;
  border-radius: 10px;
}

.raffle-section {
  text-align: center;
}

.awards-display {
  margin-bottom: 30px;
}

.awards-display h3 {
  color: #303133;
  margin-bottom: 20px;
  font-size: 20px;
}

.awards-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 15px;
  margin-bottom: 20px;
}

.award-item {
  background: white;
  border: 2px solid #e4e7ed;
  border-radius: 10px;
  padding: 20px;
  text-align: center;
  transition: all 0.3s ease;
  cursor: pointer;
}

.award-item:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.award-item.highlight {
  border-color: #409eff;
  background: #ecf5ff;
  transform: scale(1.05);
  box-shadow: 0 8px 25px rgba(64, 158, 255, 0.3);
}

.award-icon {
  font-size: 40px;
  margin-bottom: 10px;
}

.award-title {
  font-weight: bold;
  color: #303133;
  margin-bottom: 5px;
}

.award-subtitle {
  color: #909399;
  font-size: 12px;
}

.raffle-controls {
  display: flex;
  gap: 20px;
  justify-content: center;
  margin-bottom: 20px;
}

.raffle-button {
  font-size: 18px;
  padding: 15px 30px;
  border-radius: 25px;
  background: linear-gradient(45deg, #ff6b6b, #ee5a24);
  border: none;
  color: white;
  font-weight: bold;
  box-shadow: 0 4px 15px rgba(255, 107, 107, 0.3);
  transition: all 0.3s ease;
}

.raffle-button:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 8px 25px rgba(255, 107, 107, 0.4);
}

.raffle-button:disabled {
  background: #c0c4cc;
  cursor: not-allowed;
}

.armory-button {
  font-size: 16px;
  padding: 12px 24px;
  border-radius: 20px;
}

.sign-button {
  font-size: 16px;
  padding: 12px 24px;
  border-radius: 20px;
}

.result-section {
  margin-top: 20px;
}

.loading-section,
.error-section {
  margin: 20px 0;
}

@media (max-width: 768px) {
  .raffle-container {
    padding: 10px;
  }
  
  .awards-grid {
    grid-template-columns: repeat(2, 1fr);
  }
  
  .raffle-controls {
    flex-direction: column;
    align-items: center;
  }
}
</style>
