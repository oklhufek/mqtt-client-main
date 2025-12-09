package utb.fai.Simulation;

import utb.fai.API.IrrigationSystem;

/**
 * ZDE NEMENIT ZADNOU CAST KODU
 */
public class IrrigationSystemSimulation implements IrrigationSystem {

    public static final float IrrigationIntensity = 5.0f;

    private boolean active;
    private double faultProbability;
    private boolean hasFault;
    private Simulation simulation;

    public IrrigationSystemSimulation(double faultProbability, Simulation simulation) {
        this.faultProbability = faultProbability;
        this.simulation = simulation;
        this.hasFault = false;
        this.active = false;
        simulation.setIrrigationIntensity(0);
    }

    @Override
    public boolean isActive() {
        return this.active;
    }

    @Override
    public void activate() {
        if (hasFault)
            return;
        float rnd = this.simulation.getRandom().nextFloat();
        if (rnd < faultProbability) {
            hasFault = true;
            return;
        }
        active = true;
        simulation.setIrrigationIntensity(IrrigationSystemSimulation.IrrigationIntensity);
    }

    @Override
    public void deactivate() {
        if (hasFault)
            return;
        if (this.simulation.getRandom().nextFloat() < faultProbability) {
            hasFault = true;
            return;
        }
        active = false;
        simulation.setIrrigationIntensity(0);
    }

    @Override
    public boolean hasFault() {
        return hasFault;
    }
}
