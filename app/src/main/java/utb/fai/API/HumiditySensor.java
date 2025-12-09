package utb.fai.API;

/**
 * Rozhrani pro komunikaci se sensorem vlkosti
 * 
 * ZDE NEMENIT ZADNOU CAST KODU
 */
public interface HumiditySensor {

    /**
     * Tato metoda navrati aktualni hodnotu vlhkosti namerenou cidlem
     * 
     * @return Hodnota vlhkosti
     */
    public float readRAWValue();

    /**
     * Tato metoda navrati stav o to zda doslo k nejake poruce cidla vlhkosti
     * 
     * @return True v pripade vyskytu nejake poruchy
     */
    public boolean hasFault();

}
