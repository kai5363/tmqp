const Tmqp = require('../tmqp');

const tmqp = new Tmqp({ host: 'localhost', port: 25566, cluster: true });
const students = [
  'Kelvin',
  'Peter',
  'Adam',
  'Hazel',
  'Howard',
  'Claudia',
  'Alex',
  'Tim',
  'Sam',
  'Euli',
  'Ellie',
  'Domingo',
  'Jimmy',
  'Morton',
];

const getStudents = (nums) => {
  const studentList = [];
  for (let i = 0; i < nums; i++) {
    const rand = Math.floor(Math.random() * students.length);
    studentList.push(students[rand]);
  }
  return studentList;
};

const randomProduce = (queue) => {
  const times = Math.floor(Math.random() * 10) + 1;
  const interval = Math.random() * 2000;
  const studentList = getStudents(times);
  tmqp.produce(queue, studentList);
  const d = new Date();
  console.log(
    d.toLocaleDateString('en-US'),
    d.toLocaleTimeString('en-US'),
    `[produce] queue:${queue}, list:${JSON.stringify(studentList)}`,
  );
  setTimeout(() => {
    randomProduce(queue);
  }, interval);
};

const randomConsume = async (queue) => {
  const times = Math.floor(Math.random() * 10) + 1;
  const interval = Math.random() * 2000;
  const result = await tmqp.consume(queue, times);
  const d = new Date();
  console.log(
    d.toLocaleDateString('en-US'),
    d.toLocaleTimeString('en-US'),
    `[consume] queue:${queue}, list:${JSON.stringify(result)}`,
  );

  setTimeout(() => {
    randomConsume(queue);
  }, interval);
};

setTimeout(() => {
  randomProduce('PullRequests');
  randomConsume('PullRequests');
}, 1000);
