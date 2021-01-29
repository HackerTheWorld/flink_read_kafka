package flinksummary.vo;

public class SideOutput {
    private String mouldNoSys;

    public String getMouldNoSys() {
        return mouldNoSys;
    }

    public void setMouldNoSys(String mouldNoSys) {
        this.mouldNoSys = mouldNoSys;
    }

    @Override
    public String toString(){
        return mouldNoSys;
    }

}
