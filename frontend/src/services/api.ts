import axios from 'axios'

const API_BASE_URL = 'http://localhost:8080'

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json'
  }
})

const toNumber = (value: unknown): number => Number(value ?? 0)

const normalizeDrawReply = (data: any): DrawReply => ({
  award_id: toNumber(data?.award_id ?? data?.awardId),
  award_title: data?.award_title ?? data?.awardTitle ?? '',
  award_index: toNumber(data?.award_index ?? data?.awardIndex)
})

const normalizeAward = (award: any): RaffleAward => ({
  award_id: toNumber(award?.award_id ?? award?.awardId),
  award_title: award?.award_title ?? award?.awardTitle ?? '',
  award_subtitle: award?.award_subtitle ?? award?.awardSubtitle ?? '',
  sort: toNumber(award?.sort)
})

const normalizeUserActivityAccount = (data: any): UserActivityAccount => ({
  activity_id: toNumber(data?.activity_id ?? data?.activityId),
  total_count: toNumber(data?.total_count ?? data?.totalCount),
  total_count_surplus: toNumber(data?.total_count_surplus ?? data?.totalCountSurplus),
  day_count: toNumber(data?.day_count ?? data?.dayCount),
  day_count_surplus: toNumber(data?.day_count_surplus ?? data?.dayCountSurplus),
  month_count: toNumber(data?.month_count ?? data?.monthCount),
  month_count_surplus: toNumber(data?.month_count_surplus ?? data?.monthCountSurplus)
})

export interface RaffleAward {
  award_id: number
  award_title: string
  award_subtitle: string
  sort: number
}

export interface DrawRequest {
  user_id: string
  activity_id: number
}

export interface DrawReply {
  award_id: number
  award_title: string
  award_index: number
}

export interface UserActivityAccount {
  activity_id: number
  total_count: number
  total_count_surplus: number
  day_count: number
  day_count_surplus: number
  month_count: number
  month_count_surplus: number
}

export const activityAPI = {
  // 活动装配
  async armoryActivity(activityId: number): Promise<boolean> {
    const response = await api.get(`/api/v1/strategy/raffle/activity/armory?activity_id=${activityId}`)
    return response.data.success
  },

  // 加载单个用户活动账户额度到缓存
  async loadUserActivityAccount(userId: string, activityId: number): Promise<boolean> {
    const response = await api.post('/api/v1/strategy/raffle/activity/load_user_activity_account', {
      user_id: userId,
      activity_id: activityId
    })
    return Boolean(response.data.success)
  },

  // 抽奖
  async draw(userId: string, activityId: number): Promise<DrawReply> {
    const response = await api.post('/api/v1/strategy/raffle/activity/draw', {
      user_id: userId,
      activity_id: activityId
    })
    return normalizeDrawReply(response.data)
  },

  // 查询用户活动账户
  async queryUserActivityAccount(userId: string, activityId: number): Promise<UserActivityAccount> {
    const response = await api.post('/api/v1/strategy/raffle/activity/query_user_activity_account', {
      user_id: userId,
      activity_id: activityId
    })
    return normalizeUserActivityAccount(response.data)
  },

  // 签到
  async calendarSignRebate(userId: string): Promise<boolean> {
    const response = await api.post('/api/v1/strategy/raffle/activity/calendar_sign_rebate', {
      user_id: userId
    })
    return response.data.success
  },

  // 查询签到状态
  async isCalendarSignRebate(userId: string): Promise<boolean> {
    const response = await api.post('/api/v1/strategy/raffle/activity/is_calendar_sign_rebate', {
      user_id: userId
    })
    return Boolean(response.data.is_signed ?? response.data.isSigned)
  }
}

export const strategyAPI = {
  // 策略装配
  async armoryStrategy(strategyId: number): Promise<boolean> {
    const response = await api.get(`/api/v1/strategy/raffle/strategy/armory?strategy_id=${strategyId}`)
    return response.data.success
  },

  // 查询奖品列表
  async queryRaffleAwardList(strategyId: number, userId?: string): Promise<RaffleAward[]> {
    const params = new URLSearchParams()
    params.append('strategy_id', strategyId.toString())
    if (userId) {
      params.append('user_id', userId)
    }
    const response = await api.get(`/api/v1/strategy/raffle/strategy/query_raffle_award_list?${params}`)
    const awards = response.data.awards ?? response.data.strategyAwards ?? []
    return awards.map(normalizeAward)
  },

  // 随机抽奖
  async randomRaffle(strategyId: number, userId: string): Promise<DrawReply> {
    const response = await api.post('/api/v1/strategy/raffle/strategy/random_raffle', {
      strategy_id: strategyId,
      user_id: userId
    })
    return normalizeDrawReply(response.data)
  }
}
