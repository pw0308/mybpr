
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
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.DistributedMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 需要用户物品id从1开始连续
 */
public class MyBPR {

    static final Integer NUM_LFM =10;
    static final Integer NUM_ITERATIONS=100;
    static final Integer NUM_REDUCERS =10;


    public static void main(String args[]){

        //todo 读取数据
        SparkSession spark = SparkSession.builder().master("local").appName("Simple Application").getOrCreate();
        JavaRDD<String> stringJavaRDD = JavaSparkContext.fromSparkContext(spark.sparkContext())
                .textFile("/Users/zhengpeiwei/Downloads/ml-1m 2/ratings.dat");
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
        //这里用到了LocalMatrix 其实这又不是分布式的,算的时候还是把矩阵传到各个机子上做
        //所以其实你用Java的ujmp做这个矩阵也可以
        //但是若用分布式矩阵,因为其是rdd组成的,所以就有限制了(但是Spark分布式矩阵功能挺有限,根本用不到这儿)
        Matrix userMatrix = Matrices.randn(numUsers, NUM_LFM, new Random());
        Matrix itemMatrix = Matrices.randn(numItems, NUM_LFM, new Random());

        for (int i = 1; i <= NUM_ITERATIONS; i++) {
            final Matrix finalUserMatrix = userMatrix;
            final Matrix finalItemMatrix = itemMatrix;
            //对每一个partition做优化,分别更新用户和物品向量,使用mappartitons,每个分区调用一次call
            JavaRDD<Tuple2<Matrix, Matrix>> result = partitonedRatings.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Tuple2<Matrix, Matrix>>() {
                @Override
                public Iterator<Tuple2<Matrix, Matrix>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                    return sampleAndOptimizePartition(tuple2Iterator, finalUserMatrix, finalItemMatrix, numItems);
                }
            });


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
        }

        System.out.println(predict(userMatrix,itemMatrix,1,1));
        System.out.println(predict(userMatrix,itemMatrix,1,2));
        System.out.println(predict(userMatrix,itemMatrix,1,3));
        System.out.println(predict(userMatrix,itemMatrix,1,4));
        System.out.println(predict(userMatrix,itemMatrix,1,5));
        System.out.println(predict(userMatrix,itemMatrix,1,150));
    }



    //对一个partition的数据进行采样和优化
    private static Iterator<Tuple2<Matrix,Matrix>> sampleAndOptimizePartition(Iterator<Tuple2<Integer,Integer>> ratings, Matrix userMatrix, Matrix itemMatrix, int numItems) {

        //采样
        //<用户id,其看过的物品>
        Map<Integer,Set<Integer>> observed=new HashMap<>();
        //使用observed记录一个用户所看过的物品
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
        }
        //可用于生成1-numItems的随机数
        Random randomItem=new Random();
        Random randomUser=new Random();
        //observed里只有这个partition的用户,可以用来用户抽样
        Integer[] keys = observed.keySet().toArray(new Integer[0]);
        //把(u,i,0)=>(u,i,j)
        ArrayList<Tuple2<Integer,Tuple2<Integer,Integer>>> samples=new ArrayList<>();
        //生成100*User个sample
        for (int sampleCount = 0, smax = keys.length * 100; sampleCount < smax; sampleCount++) {
            //随机抽样s
            int userIdx, posItemIdx, negItemIdx;
            while (true) {
                //选取一个用户
                userIdx = keys[randomUser.nextInt(keys.length)];
                //这个用户的看过物品集
                Set<Integer> itemSet = observed.get(userIdx);
                if (itemSet.size() == 0 || itemSet.size() == numItems)
                    continue;
                //构造成List好随机抽样
                List<Integer> itemList=new ArrayList<>(itemSet);

                //从itemSet中随机抽取一个看过的物品
                //从用户行为列的List中抽取一个行为过的物品
                posItemIdx = itemList.get(randomItem.nextInt(itemList.size()));
                //随机抽取直到抽取到一个用户行为列之中没有的物品
                do {
                    negItemIdx = randomItem.nextInt(numItems)+1;
                } while (itemSet.contains(negItemIdx));
                samples.add(new Tuple2<>(userIdx,new Tuple2<Integer, Integer>(posItemIdx,negItemIdx)));
                break;
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

        Double lambdaReg=0.01,alpha=0.1;
        Double xui=0.0,xuj=0.0,x_uij;
        for(int k=0;k<NUM_LFM;k++){
            xui+=(userMatrix.apply(userId-1,k)*itemMatrix.apply(prodPos-1,k));
            xuj+=(userMatrix.apply(userId-1,k)*itemMatrix.apply(prodNeg-1,k));
        }
        x_uij=xui-xuj;Double deri=Math.exp(-x_uij)/(1+Math.exp(-x_uij));

        //更新各个向量,这里作为整体更新,而不是k的每一维分别更新
        for(int k=0;k<NUM_LFM;k++){
            //必须先取出来,不然若你更新了user,然而item的更新要用到user,则用到的就不是原来的值了
            double userFactorValue = userMatrix.apply(userId-1, k);
            //获取正物品的物品向量中一个factor现有值
            double posItemFactorValue = itemMatrix.apply(prodPos-1, k);
            //获取负物品的物品向量中一个factor现有值
            double negItemFactorValue = itemMatrix.apply(prodNeg-1, k);

            double addUser=alpha*(deri*(posItemFactorValue-negItemFactorValue)-lambdaReg*userFactorValue);
            userMatrix.update(userId-1,k,userFactorValue+addUser);

            double addPos=alpha*(deri*userFactorValue-lambdaReg*posItemFactorValue);
            itemMatrix.update(prodPos-1,k,posItemFactorValue+addPos);

            double addNeg=alpha*(deri*(-userFactorValue)-lambdaReg*negItemFactorValue);
            itemMatrix.update(prodNeg-1,k,negItemFactorValue+addNeg);

        }

    }



    /**
     * 预测分数的函数
     * @param userMatrix
     * @param itemMatrix
     */
    private static double predict(Matrix userMatrix, Matrix itemMatrix, int user, int item) {
        double score=0.0;
        int k=userMatrix.numCols();
        for(int i=0;i<k;i++){
            score+=(userMatrix.apply(user-1,i)*itemMatrix.apply(item-1,i));
        }
        return score;
    }



}
