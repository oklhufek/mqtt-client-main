package utb.fai;

import utb.fai.API.HumiditySensor;
import utb.fai.API.IrrigationSystem;
import utb.fai.Simulation.HumiditySensorSimulation;
import utb.fai.Simulation.IrrigationSystemSimulation;
import utb.fai.Simulation.Simulation;

public class App {
    public static void main(String[] args) {
        // ZDE NEMENIT ZADNOU CAST KODU

        // defaultni hodnoty parametru
        long seed = 643953953L;
        float humidityFault = 0.05f;
        float irrigationFault = 0.05f;

        // nacteni argumentu
        if (args.length > 0)
            seed = Long.parseLong(args[0]);
        if (args.length > 1)
            humidityFault = Float.parseFloat(args[1]);
        if (args.length > 2)
            irrigationFault = Float.parseFloat(args[2]);

        // definici simulace
        Simulation simulation = new Simulation(20.0f, seed);
        HumiditySensor sensor = new HumiditySensorSimulation(humidityFault, simulation);
        IrrigationSystem irrigation = new IrrigationSystemSimulation(irrigationFault, simulation);

        // spusti simulaci
        simulation.startSimulation();

        // spusteni MQTT klienta
        SoilMoistureMQTTClient client = new SoilMoistureMQTTClient(sensor, irrigation);
        client.start();
    }
}