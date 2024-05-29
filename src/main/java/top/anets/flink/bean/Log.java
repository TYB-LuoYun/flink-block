package top.anets.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author ftm
 * @date 2023-11-10 18:02
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Log {
    private String method;
    private Date date;
    private Integer count  = 1;

}
