package com.ctrip.framework.apollo.openapi.util;

import com.ctrip.framework.apollo.openapi.service.ConsumerService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ConsumerAuthUtil {
  /**
   * Request Attribute —— Consumer 编号
   */
  static final String CONSUMER_ID = "ApolloConsumerId";
  @Autowired
  private ConsumerService consumerService;

  /**
   * 获得 Token 获得对应的 Consumer 编号
   *
   * @param token Token
   * @return Consumer 编号
   */
  public Long getConsumerId(String token) {
    return consumerService.getConsumerIdByToken(token);
  }

  /**
   * 设置 Consumer 编号到 Request
   *
   * @param request 请求
   * @param consumerId Consumer 编号
   */
  public void storeConsumerId(HttpServletRequest request, Long consumerId) {
    request.setAttribute(CONSUMER_ID, consumerId);
  }

  /**
   * 获得 Consumer 编号从 Request
   *
   * @param request 请求
   * @return Consumer 编号
   */
  public long retrieveConsumerId(HttpServletRequest request) {
    Object value = request.getAttribute(CONSUMER_ID);

    try {
      return Long.parseLong(value.toString());
    } catch (Throwable ex) {
      throw new IllegalStateException("No consumer id!", ex);
    }
  }
}
