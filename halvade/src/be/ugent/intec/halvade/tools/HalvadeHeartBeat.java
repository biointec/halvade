/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.tools;

import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import be.ugent.intec.halvade.utils.Logger;
import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 *
 * @author ddecap
 */
public class HalvadeHeartBeat extends Thread {
    protected int interval = 60000;
    protected Context context;
    protected boolean stopBeating = false;

    public HalvadeHeartBeat(Context context) {
        this.context = context;
    }   
    
    public HalvadeHeartBeat(Context context, int interval) {
        this.interval = interval;
        this.context = context;
    }
    
    @Override
    public void run() {
        try {
            while(!stopBeating) {
                Thread.sleep(interval);
                context.getCounter(HalvadeCounters.STILL_RUNNING_HEARTBEAT).increment(1);
                context.progress();
            }
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        }
        
    }

    void jobFinished() {
        stopBeating = true;
    }
}