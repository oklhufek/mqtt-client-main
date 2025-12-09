package utb.fai.Simulation;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ZDE NEMENIT ZADNOU CAST KODU
 */
public class Simulation {

    private volatile float currentMoistureLevel; // aktualni uroven vlhkosti pudy
    private float irrigationIntensity = 0; // intenzita zavlazovani
    private float temperature = 20; // pocatecni teplota v °C
    private long lastUpdateTime; // cas posledni aktualizace

    private Random random;

    private ScheduledExecutorService executor;

    public Simulation(float initialMoistureLevel, long seed) {
        this.currentMoistureLevel = Math.max(0, Math.min(initialMoistureLevel, 100));
        this.random = new Random(seed);
        this.lastUpdateTime = System.currentTimeMillis();
        this.executor = Executors.newScheduledThreadPool(1);
        this.temperature = (float) (this.random.nextFloat() * 30.0f) + 15.0f;
    }

    public void startSimulation() {
        this.executor.scheduleAtFixedRate(this::update, 0, 250, TimeUnit.MILLISECONDS);
        System.err.printf("Temperature = %f\n Current Moisture Level = %f %%\n", this.temperature, this.currentMoistureLevel);
        System.err.println("Simulation running...");
    }

    public void stopSimulation() {
        this.executor.shutdown();
    }

    private void update() {
        long currentTime = System.currentTimeMillis();
        float deltaTime = (currentTime - lastUpdateTime) / 1000.0f;
        lastUpdateTime = currentTime;

        // simulace zmeny vlhkosti
        float evaporationRate = calculateEvaporationRate();
        currentMoistureLevel += deltaTime * (irrigationIntensity - evaporationRate);

        // omezeni hodnot vlhkosti
        if (currentMoistureLevel > 100) {
            currentMoistureLevel = 100;
        } else if (currentMoistureLevel < 0) {
            currentMoistureLevel = 0;
        }

        System.err.println("Updated moisture level: " + currentMoistureLevel + ", Delta time: " + deltaTime + "s");
    }

    /**
     * Vypocet intenzity odparovani
     */
    private float calculateEvaporationRate() {
        // definice dolni a horni meze
        float lowerTemp = 15f, upperTemp = 45f;
        float lowerRate = 0.05f, upperRate = 0.4f;

        // hranicni hodnoty
        if (temperature <= lowerTemp)
            return lowerRate;
        if (temperature >= upperTemp)
            return upperRate;

        // linearni interpolace
        return lowerRate + (upperRate - lowerRate) * (temperature - lowerTemp) / (upperTemp - lowerTemp);
    }

    /**
     * Nastavení intenzity zavlažování (předpokládejme, že je voláno externě)
     * 
     * @param intensity Intenzita zavlazovani
     */
    public void setIrrigationIntensity(float intensity) {
        this.irrigationIntensity = intensity;
    }

    /**
     * Nastavení teploty (předpokládejme, že je voláno externě)
     * 
     * @param temperature Teplota
     */
    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public float getCurrentMoistureLevel() {
        return currentMoistureLevel;
    }

    public float getIrrigationIntensity() {
        return irrigationIntensity;
    }

    public float getTemperature() {
        return temperature;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    /**
     * Pseudonahodny generator cisel. Muzou pouzivat jen pouze simulovane zarizeni.
     * Nepouzivat v kodu!
     */
    public Random getRandom() {
        return random;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }
}
