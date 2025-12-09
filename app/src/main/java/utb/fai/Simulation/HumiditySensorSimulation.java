package utb.fai.Simulation;

import utb.fai.API.HumiditySensor;

/**
 * ZDE NEMENIT ZADNOU CAST KODU
 */
public class HumiditySensorSimulation implements HumiditySensor {
    private double faultProbability;
    private boolean hasFault;
    private Simulation simulation;

    public HumiditySensorSimulation(double faultProbability, Simulation simulation) {
        this.faultProbability = faultProbability;
        this.simulation = simulation;
        this.hasFault = false;
    }

    @Override
    public float readRAWValue() {
        if (hasFault)
            return -1;
        float rnd = this.simulation.getRandom().nextFloat();
        System.out.println(rnd);
        if (rnd < faultProbability) {
            hasFault = true;
            return -1;
        }
        return simulation.getCurrentMoistureLevel();
    }

    @Override
    public boolean hasFault() {
        return hasFault;
    }
}
