package io.datavines.server.repository.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.datavines.server.repository.entity.Server;
import io.datavines.server.repository.mapper.ServerMapper;
import io.datavines.server.repository.service.ServerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service("serverService")
public class ServerServiceImpl extends ServiceImpl<ServerMapper, Server> implements ServerService {
    @Override
    public Server getOne() {
        return baseMapper.getOne();
    }
}
