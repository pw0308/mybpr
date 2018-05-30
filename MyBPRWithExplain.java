
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class MyBPRWithExplain {

    static final Integer NUM_LFM =3;
    static final Integer NUM_ITERATIONS=1;
    static final Integer NUM_OF_NEGATIVE_PER_IMPLICIT = 1;
    static final Integer NUM_REDUCERS =2;


    public static void main(String args[]){

        //todo 读取数据
        SparkSession spark = SparkSession.builder().master("local").appName("Simple Application").getOrCreate();
        JavaRDD<String> stringJavaRDD = JavaSparkContext.fromSparkContext(spark.sparkContext())
                .textFile("/Users/zhengpeiwei/Desktop/data.dat");
        JavaRDD<User_Behavior> user_behaviorJavaRDD=stringJavaRDD.map(new Function<String, User_Behavior>() {
            @Override
            public User_Behavior call(String v1) throws Exception {
                String[] split = v1.split("::");
                return new User_Behavior(split[0],split[1],"1");
            }
        });
        JavaPairRDD<Integer, Integer> ratings = user_behaviorJavaRDD.mapToPair(new PairFunction<User_Behavior, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(User_Behavior user_behavior) throws Exception {
                return new Tuple2<>(Integer.parseInt(user_behavior.getUserId()), Integer.parseInt(user_behavior.getItemId()));
            }
        });
        JavaPairRDD<Integer, Integer> partitonedRatings = ratings.partitionBy(new HashPartitioner(NUM_REDUCERS)).persist(StorageLevel.MEMORY_AND_DISK());

        //todo 求用户物品数
        final int numUsers=(int)partitonedRatings.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._1;
            }
        }).distinct().max(new DummyComparator());
        final int numItems=(int)partitonedRatings.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._2;
            }
        }).distinct().max(new DummyComparator());
        System.out.println("!!!!!!!!!!!!!!!!   "+ numUsers+" "+numItems);

        //todo 构造用户物品隐因子矩阵
        Matrix userMatrix = Matrices.randn(numUsers, NUM_LFM, new Random());
        Matrix itemMatrix = Matrices.randn(numItems, NUM_LFM, new Random());

        //打印最开始的时候的用户物品矩阵
        for(int r=0;r<userMatrix.numRows();r++){
            for(int c=0;c<userMatrix.numCols();c++){
                System.out.print(userMatrix.apply(r,c)+" ");
            }
            System.out.println();
        }
        System.out.println();
        for(int r=0;r<itemMatrix.numRows();r++){
            for(int c=0;c<itemMatrix.numCols();c++){
                System.out.print(itemMatrix.apply(r,c)+" ");
            }
            System.out.println();
        }



        for (int i = 1; i <= NUM_ITERATIONS; i++) {
            final Matrix finalUserMatrix = userMatrix;
            final Matrix finalItemMatrix = itemMatrix;
            final int finalNumItems=numItems;
            //对每一个partition做优化,分别更新用户和物品向量,使用mappartitons,每个分区调用一次call
            JavaRDD<Tuple2<Matrix, Matrix>> result = partitonedRatings.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Tuple2<Matrix, Matrix>>() {
                @Override
                //todo 直接传入原matrix会不会不行 需要串副本??
                public Iterator<Tuple2<Matrix, Matrix>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                    return sampleAndOptimizePartition(tuple2Iterator, finalUserMatrix, finalItemMatrix, finalNumItems);
                }
            });


            //计算有多少个Partitions
            //把所有矩阵相加,下面再求平均
            Tuple2<Matrix, Matrix> averagedMatrices = result.reduce(new Function2<Tuple2<Matrix, Matrix>, Tuple2<Matrix, Matrix>, Tuple2<Matrix, Matrix>>() {
                @Override
                public Tuple2<Matrix, Matrix> call(Tuple2<Matrix, Matrix> v1, Tuple2<Matrix, Matrix> v2) throws Exception {
                    Matrix userM1 = v1._1,userM2 = v2._1,itemM1 = v1._2,itemM2 = v2._2;

                    double[]values=new double[numUsers*NUM_LFM];
                    //注意是按列存的
                    int count=0;
                    for(int c=0;c<NUM_LFM;c++){
                        for(int r=0;r<numUsers;r++){
                            values[count]=userM1.apply(r,c)+userM2.apply(r,c);
                            count++;
                        }
                    }
                    Matrix newu = Matrices.dense(numUsers, NUM_LFM, values);

                    double[]values1=new double[numItems*NUM_LFM];
                    count=0;
                    //注意是按列存的
                    for(int c=0;c<NUM_LFM;c++){
                        for(int r=0;r<numItems;r++){
                            values1[count]=itemM1.apply(r,c)+itemM2.apply(r,c);
                            count++;
                        }
                    }
                    Matrix newi = Matrices.dense(numItems, NUM_LFM, values1);
                    return new Tuple2<Matrix, Matrix>(newu,newi);
                }
            });

            //todo 在此验证两个矩阵相加的结果
            System.out.println("下面是两个矩阵相加的结果");
            for(int r=0;r<averagedMatrices._1.numRows();r++){
                for(int c=0;c<averagedMatrices._1.numCols();c++){
                    System.out.print(averagedMatrices._1.apply(r,c)+" ");
                }
                System.out.println();
            }
            System.out.println();System.out.println();
            for(int r=0;r<averagedMatrices._2.numRows();r++){
                for(int c=0;c<averagedMatrices._2.numCols();c++){
                    System.out.print(averagedMatrices._2.apply(r,c)+" ");
                }
                System.out.println();
            }
            System.out.println();System.out.println();System.out.println();System.out.println();System.out.println();System.out.println();


            //todo 求平均
            for(int r=0;r<numUsers;r++){
                for(int c=0;c<NUM_LFM;c++){
                    averagedMatrices._1.update(r,c,averagedMatrices._1.apply(r,c)/ NUM_REDUCERS);
                }
            }
            userMatrix = averagedMatrices._1;
            for(int r=0;r<numItems;r++){
                for(int c=0;c<NUM_LFM;c++){
                    averagedMatrices._2.update(r,c,averagedMatrices._2.apply(r,c)/ NUM_REDUCERS);
                }
            }
            itemMatrix = averagedMatrices._2;


            System.out.println("下面是相加后/2求均值的结果");
            for(int r=0;r<userMatrix.numRows();r++){
                for(int c=0;c<userMatrix.numCols();c++){
                    System.out.print(userMatrix.apply(r,c)+" ");
                }
                System.out.println();
            }
            System.out.println();System.out.println();
            for(int r=0;r<itemMatrix.numRows();r++){
                for(int c=0;c<itemMatrix.numCols();c++){
                    System.out.print(itemMatrix.apply(r,c)+" ");
                }
                System.out.println();
            }

        }
    }



    //对一个partition的数据进行采样和优化
    private static Iterator<Tuple2<Matrix,Matrix>> sampleAndOptimizePartition(Iterator<Tuple2<Integer,Integer>> ratings, Matrix userMatrix, Matrix itemMatrix, int numItems) {

        //采样,构造数据结构
        // todo 本来想实现像librec一样实现用户抽样,但是因为这是partition,所以用户是分片的,不好对整体抽样
        ArrayList<Tuple2<Integer,Tuple2<Integer,Integer>>> list=new ArrayList<>();
        //<用户id,其看过的物品>
        Map<Integer,Set<Integer>> observed=new HashMap<>();
        //对每一个反馈,构造(u,i,0)三元组,并且使用observed记录一个用户所看过的物品
        for (Iterator<Tuple2<Integer, Integer>> it = ratings; it.hasNext();) {
            Tuple2<Integer, Integer> one = it.next();
            if(!observed.containsKey(one._1)){
                HashSet<Integer> items = new HashSet<>();
                items.add(one._2);
                observed.put(one._1,items);
            }
            else{
                observed.get(one._1).add(one._2);
            }
            list.add(new Tuple2<>(one._1,new Tuple2<Integer, Integer>(one._2,0)));
        }
        //可用于生成1-numItems的随机数
        Random random=new Random();
        //把(u,i,0)=>(u,i,j)
        ArrayList<Tuple2<Integer,Tuple2<Integer,Integer>>> samples=new ArrayList<>();
        for(Tuple2<Integer,Tuple2<Integer,Integer>> sample:list){
            Set<Integer> observes = observed.get(sample._1);
            int negItem;
            for(int count=1;count<=NUM_OF_NEGATIVE_PER_IMPLICIT;count++){
                do {
                    negItem = random.nextInt(numItems)+1;
                } while (observes.contains(negItem));
                samples.add(new Tuple2<>(sample._1,new Tuple2<Integer, Integer>(sample._2._1,negItem)));
                count++;
            }
        }
        //训练
        for(Tuple2<Integer,Tuple2<Integer,Integer>> sample:samples){
            gradientSinglePoint(sample._1, sample._2._1, sample._2._2, userMatrix, itemMatrix);
        }
        ArrayList<Tuple2<Matrix, Matrix>> result=new ArrayList<>();

        result.add(new Tuple2<Matrix,Matrix>(userMatrix,itemMatrix));
        return  result.iterator();
    }


    private static void gradientSinglePoint(Integer userId, Integer prodPos, Integer prodNeg, Matrix userMatrix, Matrix itemMatrix) {


        System.out.println("下降前的矩阵");
        for(int r=0;r<userMatrix.numRows();r++){
            for(int c=0;c<userMatrix.numCols();c++){
                System.out.print(userMatrix.apply(r,c)+" ");
            }
            System.out.println();
        }
        System.out.println();
        for(int r=0;r<itemMatrix.numRows();r++){
            for(int c=0;c<itemMatrix.numCols();c++){
                System.out.print(itemMatrix.apply(r,c)+" ");
            }
            System.out.println();
        }
        System.out.println(); System.out.println();





        Double lambdaReg=0.01,alpha=0.1;
        double xui=0.0,xuj=0.0,x_uij;
        for(int k=0;k<NUM_LFM;k++){
            //System.out.println(" xui+= "+userMatrix.apply(userId-1,k)+"*"+itemMatrix.apply(prodPos-1,k)+"="+userMatrix.apply(userId-1,k)*itemMatrix.apply(prodPos-1,k) );
            xui+=(userMatrix.apply(userId-1,k)*itemMatrix.apply(prodPos-1,k));
            //System.out.println(" xuj+= "+userMatrix.apply(userId-1,k)+"*"+itemMatrix.apply(prodNeg-1,k)+"="+userMatrix.apply(userId-1,k)*itemMatrix.apply(prodNeg-1,k) );
            xuj+=(userMatrix.apply(userId-1,k)*itemMatrix.apply(prodNeg-1,k));
        }

        //System.out.println(xui+" "+xuj);
        x_uij=xui-xuj;Double deri=Math.exp(-x_uij)/(1+Math.exp(-x_uij));
        System.out.println("样本是 userid= "+userId+" prodId= "+prodPos+" , "+prodNeg+" xuij= "+x_uij+" ,deri= "+deri);

        //更新各个向量,这里作为整体更新,而不是k的每一维分别更新
        for(int k=0;k<NUM_LFM;k++){
            //必须先取出来,不然若你更新了user,然而item的更新要用到user,则用到的就不是原来的值了
            double userFactorValue = userMatrix.apply(userId-1, k);
            //获取正物品的物品向量中一个factor现有值
            double posItemFactorValue = itemMatrix.apply(prodPos-1, k);
            //获取负物品的物品向量中一个factor现有值
            double negItemFactorValue = itemMatrix.apply(prodNeg-1, k);

            double addUser=alpha*(deri*(posItemFactorValue-negItemFactorValue)-lambdaReg*userFactorValue);
            userMatrix.update(userId-1,k,userMatrix.apply(userId-1,k)+addUser);

            double addPos=alpha*(deri*userFactorValue-lambdaReg*posItemFactorValue);
            itemMatrix.update(prodPos-1,k,itemMatrix.apply(prodPos-1,k)+addPos);

            double addNeg=alpha*(deri*userFactorValue-lambdaReg*negItemFactorValue);
            itemMatrix.update(prodNeg-1,k,itemMatrix.apply(prodNeg-1,k)+addNeg);

        }

        System.out.println("下降后的矩阵");
        for(int r=0;r<userMatrix.numRows();r++){
            for(int c=0;c<userMatrix.numCols();c++){
                System.out.print(userMatrix.apply(r,c)+" ");
            }
            System.out.println();
        }
        System.out.println();
        for(int r=0;r<itemMatrix.numRows();r++){
            for(int c=0;c<itemMatrix.numCols();c++){
                System.out.print(itemMatrix.apply(r,c)+" ");
            }
            System.out.println();
        }
        System.out.println(); System.out.println();
    }





}
