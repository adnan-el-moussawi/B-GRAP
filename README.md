# B-GRAP : Balanced GRAph Partitioning algorithm for large graphs

The definition of effective strategies for graph partitioning is a major challenge in distributed environments since an effective graph partitioning allows to considerably improve the performance of large graph data analytics computations. In this paper, we propose a multi-objective and scalable Balanced GRAph Partitioning (B-GRAP) algorithm, based on Label Propagation (LP) approach, to produce balanced graph partitions. B-GRAP defines a new efficient initialization procedure and different objective functions to deal with either vertex or edge balance constraints while considering edge direction in graphs. B-GRAP is implemented of top of the open source distributed graph processing system Giraph. The experiments are performed on various graphs with different structures and sizes (going up to 50.6M vertices and 1.9B edges) while varying the number of partitions. We evaluate B-GRAP using several quality measures and the computation time. The results show that B-GRAP (i) provides a good balance while reducing the cuts between the different computed partitions (ii) reduces the global computation time, compared to LP-based algorithms. 


Cite this repository as: 
El Moussawi, Adnan, Nacera Bennacer Seghouani, and Francesca Bugiotti. "B-GRAP: Balanced GRAph Partitioning Algorithm for Large Graphs." Journal of Data Intelligence 2.2 (2021): 116-135.

El Moussawi, Adnan, Nacéra Bennacer Seghouani, and Francesca Bugiotti. "A Graph Partitioning Algorithm for Edge or Vertex Balance." International Conference on Database and Expert Systems Applications. Springer, Cham, 2020.

Bibtex citations: 
@article{elmoussawi2021,
  TITLE = {{B-GRAP: Balanced GRAph Partitioning Algorithm for Large Graphs}},
  AUTHOR = {El Moussawi, Adnan and Seghouani, Nacera Bennacer and Bugiotti, Francesca},
  URL = {http://www.rintonpress.com/xjdi2/xjdi2-2/116-135.pdf},
  JOURNAL = {{Journal of Data Intelligence}},
  PUBLISHER = {{Rinton Press}},
  VOLUME = {2},
  NUMBER = {2},
  PAGES = {116-135},
  YEAR = {2021},
  MONTH = Jun,
  DOI = {10.26421/JDI2.2-2},
}

@inproceedings{elmoussawi2020,
  title={A Graph Partitioning Algorithm for Edge or Vertex Balance},
  author={El Moussawi, Adnan and Seghouani, Nac{\'e}ra Bennacer and Bugiotti, Francesca},
  booktitle={International Conference on Database and Expert Systems Applications},
  pages={23--37},
  year={2020},
  organization={Springer}
}



The "giraph.lri.lahdak.*" packages contain the main paratitioning algorithms, analytic algorithms, formatters, etc.
The "running_example" package provides a Java class to configure a Giraph Job with a Hadoop cluster setting and a Java class to run an example of BGRAP algorithm.

