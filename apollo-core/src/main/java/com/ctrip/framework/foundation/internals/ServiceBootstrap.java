package com.ctrip.framework.foundation.internals;

import java.util.Iterator;
import java.util.ServiceLoader;

public class ServiceBootstrap {
  /**
   * 加载指定服务的首个对象
   *
   * @param clazz 服务类
   * @param <S> 泛型
   * @return 对象
   */
  public static <S> S loadFirst(Class<S> clazz) {
    Iterator<S> iterator = loadAll(clazz);
    if (!iterator.hasNext()) {
      throw new IllegalStateException(String.format(
          "No implementation defined in /META-INF/services/%s, please check whether the file exists and has the right implementation class!",
          clazz.getName()));
    }
    return iterator.next();
  }

  /**
   * 基于 JDK SPI ，加载指定类的所有对象
   *
   * @param clazz 服务类
   * @param <S> 泛型
   * @return 所有对象
   */
  public static <S> Iterator<S> loadAll(Class<S> clazz) {
    ServiceLoader<S> loader = ServiceLoader.load(clazz);

    return loader.iterator();
  }
}
