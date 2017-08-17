package com.di.mesa.plugin.storm;

import backtype.storm.topology.*;

/**
 * Created by Davi on 17/8/17.
 */
public class MesaTopologyBuilder extends TopologyBuilder {

    private MesaConfig config;

    public MesaTopologyBuilder(MesaConfig config) {
        this.config = config;
    }

    public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
        BoltDeclarer boltDeclarer = super.setBolt(id, bolt, null);

        if (config.shouldEnableNotifer()) {
            return boltDeclarer.allGrouping(config.getNotiferComponentId(), config.getNotiferStreamingId());
        }
        return boltDeclarer;
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt) {
        BoltDeclarer boltDeclarer = super.setBolt(id, bolt);
        if (config.shouldEnableNotifer()) {
            return boltDeclarer.allGrouping(config.getNotiferComponentId(), config.getNotiferStreamingId());
        }

        return boltDeclarer;
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
        BoltDeclarer boltDeclarer = super.setBolt(id, bolt, parallelism_hint);
        if (config.shouldEnableNotifer()) {
            return boltDeclarer.allGrouping(config.getNotiferComponentId(), config.getNotiferStreamingId());
        }

        return boltDeclarer;
    }

    @Override
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
        BoltDeclarer boltDeclarer = super.setBolt(id, bolt, parallelism_hint);
        if (config.shouldEnableNotifer()) {
            return boltDeclarer.allGrouping(config.getNotiferComponentId(), config.getNotiferStreamingId());
        }

        return boltDeclarer;
    }


}
